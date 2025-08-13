#ifndef MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
#define MEDIA_MICROSERVICES_MOVIEIDHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>
#include <nlohmann/json.hpp>
#include "../utils_couchdb.h"

#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>

#include "../../gen-cpp/MovieIdService.h"
#include "../../gen-cpp/ComposeReviewService.h"
#include "../../gen-cpp/RatingService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"


namespace media_service {

using json = nlohmann::json;

class MovieIdHandler : public MovieIdServiceIf {
 public:
   enum class BackendType { CouchDB, DynamoDB };

  MovieIdHandler(
      memcached_pool_st *,
      mongoc_client_pool_t *,
      ClientPool<ThriftClient<ComposeReviewServiceClient>> *,
      ClientPool<ThriftClient<RatingServiceClient>> *,
      Aws::DynamoDB::DynamoDBClient*,
      std::string,
      std::string,
      std::string,
      BackendType);
  ~MovieIdHandler() override = default;
  void UploadMovieId(int64_t, const std::string &, int32_t,
                     const std::map<std::string, std::string> &) override;
  void RegisterMovieId(int64_t, const std::string &, const std::string &,
                       const std::map<std::string, std::string> &) override;

 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  ClientPool<ThriftClient<ComposeReviewServiceClient>> *_compose_client_pool;
  ClientPool<ThriftClient<RatingServiceClient>> *_rating_client_pool;
  Aws::DynamoDB::DynamoDBClient *_dynamo_client;
  std::string _aws_region;
  std::string _dynamo_table_name;
  std::string _couchdb_url;
  BackendType _backend;
};

MovieIdHandler::MovieIdHandler(
    memcached_pool_st *memcached_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    ClientPool<ThriftClient<ComposeReviewServiceClient>> *compose_client_pool,
    ClientPool<ThriftClient<RatingServiceClient>> *rating_client_pool,
    Aws::DynamoDB::DynamoDBClient *dynamo_client,
    std::string aws_region,
    std::string dynamo_table_name,
    std::string couchdb_url,
    BackendType backend) {
  _memcached_client_pool = memcached_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _compose_client_pool = compose_client_pool;
  _rating_client_pool = rating_client_pool;
  _dynamo_client = dynamo_client;
  _aws_region = aws_region;
  _dynamo_table_name = dynamo_table_name;
  _couchdb_url = couchdb_url;
  _backend = backend;
}

void MovieIdHandler::UploadMovieId(
    int64_t req_id,
    const std::string &title,
    int32_t rating,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UploadMovieId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "REQUEST to upload movie id (title=" << title << ", rating=" << rating << ")";


  memcached_return_t memcached_rc;
  memcached_st *memcached_client = memcached_pool_pop(
      _memcached_client_pool, true, &memcached_rc);
  if (!memcached_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  size_t movie_id_size;
  uint32_t memcached_flags;
  // Look for the movie id from memcached

  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetMovieId", { opentracing::ChildOf(&span->context()) });

  char* movie_id_mmc = memcached_get(
      memcached_client,
      title.c_str(),
      title.length(),
      &movie_id_size,
      &memcached_flags,
      &memcached_rc);
  if (!movie_id_mmc && memcached_rc != MEMCACHED_NOTFOUND) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = memcached_strerror(memcached_client, memcached_rc);
    memcached_pool_push(_memcached_client_pool, memcached_client);
    throw se;
  }
  get_span->Finish();
  memcached_pool_push(_memcached_client_pool, memcached_client);
  std::string movie_id_str;

  // If cached in memcached
  if (movie_id_mmc) {
    LOG(debug) << "Get movie_id " << movie_id_mmc
        << " cache hit from Memcached";
    movie_id_str = std::string(movie_id_mmc);
    free(movie_id_mmc);
  }

  // if not cached in memcached
  else {
    if (_backend == BackendType::CouchDB) {
      // --------------------------------------------------
      // ----------------- COUCHDB WRITE ------------------
      // --------------------------------------------------
      auto get_span = opentracing::Tracer::Global()->StartSpan(
          "CouchDBGetMovieId", { opentracing::ChildOf(&span->context()) });
  
      const std::string url = _couchdb_url + title;
      std::string resp;
  
      try {
        resp = couchdb_get(url);
        LOG(debug) << "found mapping for title=" << title << " in CouchDB";
      } catch (const std::exception &e) {
        LOG(error) << "movie " << title << " not found in CouchDB: " << e.what();
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Movie " + title + " not found in CouchDB";
        throw se;
      }
  
      auto j = json::parse(resp);
      if (!j.contains("movie_id") || !j["movie_id"].is_string()) {
        LOG(error) << "attribute movie_id is missing in CouchDB doc for title=" << title;
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Attribute movie_id missing in CouchDB doc";
        throw se;
      }
      get_span->Finish();
  
      movie_id_str = j["movie_id"].get<std::string>();
      LOG(debug) << "found movie " << movie_id_str << " cache miss (CouchDB)";
    }

    else if (_backend == BackendType::DynamoDB) {
      // --------------------------------------------------
      // ---------------- DYNAMODB WRITE ------------------
      // --------------------------------------------------
      auto get_span = opentracing::Tracer::Global()->StartSpan(
          "DynamoGetMovieId", { opentracing::ChildOf(&span->context()) });
  
      Aws::DynamoDB::Model::GetItemRequest get_req;
      get_req.SetTableName(_dynamo_table_name);
  
      Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> key;
      Aws::DynamoDB::Model::AttributeValue title_key;
      title_key.SetS(title);
      key.emplace("title", std::move(title_key));
      get_req.SetKey(std::move(key));
  
      auto response = _dynamo_client->GetItem(get_req);
      get_span->Finish();
  
      if (!response.IsSuccess()) {
        const auto& err = response.GetError();
        LOG(error) << "failed to get item from dynamo (region=" << _aws_region << ", table= " << _dynamo_table_name << "): " << err.GetMessage();
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = err.GetMessage();
        free(movie_id_mmc);
        throw se;
      }
  
      const auto& item = response.GetResult().GetItem();
      if (item.empty()) {
        // title not found
        LOG(error) << "movie " << title << " not found in dynamodb";
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Movie " + title + " not found in DynamoDB";
        free(movie_id_mmc);
        throw se;
      }

      auto it = item.find("movie_id");
      if (it == item.end()) {
        LOG(error) << "attribute movie_id does not exist in item";
        ServiceException se;
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Attribute movie_id does not exist in item";
        free(movie_id_mmc);
        throw se;
      }

      movie_id_str = it->second.GetS();
      LOG(debug) << "found movie " << movie_id_str << " cache miss (dynamodb)";
      // --------------------------------------------------
      // --------------------------------------------------
    }

  }
  
  std::future<void> set_future;
  std::future<void> movie_id_future;
  std::future<void> rating_future;
  set_future = std::async(std::launch::async, [&]() {
    memcached_client = memcached_pool_pop(
        _memcached_client_pool, true, &memcached_rc);
    auto set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetMovieId", { opentracing::ChildOf(&span->context()) });
    // Upload the movie id to memcached
    memcached_rc = memcached_set(
        memcached_client,
        title.c_str(),
        title.length(),
        movie_id_str.c_str(),
        movie_id_str.length(),
        static_cast<time_t>(0),
        static_cast<uint32_t>(0)
    );
    set_span->Finish();
    if (memcached_rc != MEMCACHED_SUCCESS) {
      LOG(warning) << "Failed to set movie_id to Memcached: "
                   << memcached_strerror(memcached_client, memcached_rc);
    }
    memcached_pool_push(_memcached_client_pool, memcached_client);    
  });

  movie_id_future = std::async(std::launch::async, [&]() {
    auto compose_client_wrapper = _compose_client_pool->Pop();
    if (!compose_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to compose-review-service";
      throw se;
    }
    auto compose_client = compose_client_wrapper->GetClient();
    try {
      compose_client->UploadMovieId(req_id, movie_id_str, writer_text_map);
    } catch (...) {
      _compose_client_pool->Push(compose_client_wrapper);
      LOG(error) << "Failed to upload movie_id to compose-review-service";
      throw;
    }
    _compose_client_pool->Push(compose_client_wrapper);
  });

  rating_future = std::async(std::launch::async, [&]() {
    auto rating_client_wrapper = _rating_client_pool->Pop();
    if (!rating_client_wrapper) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_CONN_ERROR;
      se.message = "Failed to connected to rating-service";
      throw se;
    }
    auto rating_client = rating_client_wrapper->GetClient();
    try {
      rating_client->UploadRating(req_id, movie_id_str, rating, writer_text_map);
    } catch (...) {
      _rating_client_pool->Push(rating_client_wrapper);
      LOG(error) << "Failed to upload rating to rating-service";
      throw;
    }
    _rating_client_pool->Push(rating_client_wrapper);
  });

  try {
    movie_id_future.get();
    rating_future.get();
    set_future.get();
  } catch (...) {
    throw;
  }

  LOG(info) << "OK (title=" << title << ", rating=" << rating << ")";
  span->Finish();
}

void MovieIdHandler::RegisterMovieId (
    const int64_t req_id,
    const std::string &title,
    const std::string &movie_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "RegisterMovieId",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "REQUEST to register movie id (title=" << title << ", movie_id=" << movie_id << ")";

  json doc;
  doc["_id"] = title;
  doc["title"] = title;
  doc["movie_id"] = movie_id;
  const std::string url = _couchdb_url + title;

  if (_backend == BackendType::CouchDB) {
    // --------------------------------------------------
    // ----------------- COUCHDB WRITE ------------------
    // --------------------------------------------------
    try {
      couchdb_put_if_absent(url, doc.dump());
      LOG(info) << "registered movie mapping in CouchDB (title=" << title
                << ", movie_id=" << movie_id << ")";
    } catch (const std::exception &e) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = std::string("Failed to register movie mapping in CouchDB: ") + e.what();
      throw se;
    }
  }
  else if (_backend == BackendType::DynamoDB) {
    // --------------------------------------------------
    // ---------------- DYNAMODB WRITE ------------------
    // --------------------------------------------------
    Aws::DynamoDB::Model::PutItemRequest req;
    req.SetTableName(_dynamo_table_name);
  
    Aws::DynamoDB::Model::AttributeValue title_attr;
    title_attr.SetS(title);
    req.AddItem("title", title_attr);
  
    Aws::DynamoDB::Model::AttributeValue movie_id_attr;
    movie_id_attr.SetS(movie_id);
    req.AddItem("movie_id", movie_id_attr);
  
    Aws::DynamoDB::Model::AttributeValue region_attr;
    region_attr.SetS(_aws_region);
    req.AddItem("region", region_attr);
  
    req.SetConditionExpression("attribute_not_exists(title)");
  
    auto response = _dynamo_client->PutItem(req);
  
    if (!response.IsSuccess()) {
      const auto& err = response.GetError();
      LOG(error) << "failed to insert to dynamo (region=" << _aws_region << ", table= " << _dynamo_table_name << "): " << err.GetMessage();
      ServiceException se;
      if (err.GetErrorType() == Aws::DynamoDB::DynamoDBErrors::CONDITIONAL_CHECK_FAILED) {
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = "Movie " + title + " already existed in DynamoDB";
      } else {
        se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
        se.message = err.GetMessage();
      }
      throw se;
    }
  }
  
  LOG(info) << "OK (title=" << title << ", movie_id=" << movie_id << ")";
  span->Finish();
}
} // namespace media_service

#endif //MEDIA_MICROSERVICES_MOVIEIDHANDLER_H
