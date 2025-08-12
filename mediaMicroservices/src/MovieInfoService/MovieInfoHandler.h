#ifndef MEDIA_MICROSERVICES_SRC_MOVIEINFOSERVICE_MOVIEINFOHANDLER_H_
#define MEDIA_MICROSERVICES_SRC_MOVIEINFOSERVICE_MOVIEINFOHANDLER_H_

#include <iostream>
#include <string>

#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <mongoc.h>
#include <bson/bson.h>
#include <nlohmann/json.hpp>
#include "../utils_couchdb.h"

#include "../../gen-cpp/MovieInfoService.h"
#include "../logger.h"
#include "../tracing.h"

namespace media_service {
using json = nlohmann::json;

class MovieInfoHandler : public MovieInfoServiceIf {
 public:
  MovieInfoHandler(
      memcached_pool_st *,
      mongoc_client_pool_t *,
      std::string);
  ~MovieInfoHandler() override = default;
  void ReadMovieInfo(MovieInfo& _return, int64_t req_id,
      const std::string& movie_id,
      const std::map<std::string, std::string> & carrier) override;
  void WriteMovieInfo(int64_t req_id, const std::string& movie_id, 
      const std::string& title, const std::vector<Cast> & casts,
      int64_t plot_id, const std::vector<std::string> & thumbnail_ids,
      const std::vector<std::string> & photo_ids,
      const std::vector<std::string> & video_ids,
      const std::string &avg_rating, int32_t num_rating,
      const std::map<std::string, std::string> & carrier) override;
  void UpdateRating(int64_t req_id, const std::string& movie_id,
      int32_t sum_uncommitted_rating, int32_t num_uncommitted_rating,
      const std::map<std::string, std::string> & carrier) override;


 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  std::string _couchdb_url;
};

MovieInfoHandler::MovieInfoHandler(
    memcached_pool_st *memcached_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    std::string couchdb_url) {
  _memcached_client_pool = memcached_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _couchdb_url = couchdb_url;
}

void MovieInfoHandler::WriteMovieInfo(
    int64_t req_id,
    const std::string &movie_id,
    const std::string &title,
    const std::vector<Cast> &casts,
    int64_t plot_id,
    const std::vector<std::string> &thumbnail_ids,
    const std::vector<std::string> &photo_ids,
    const std::vector<std::string> &video_ids,
    const std::string & avg_rating,
    int32_t num_rating,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "WriteMovieInfo",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  json new_doc;
  new_doc["_id"] = movie_id;
  new_doc["movie_id"] = movie_id;
  new_doc["title"] = title;
  new_doc["plot_id"] = plot_id;
  new_doc["avg_rating"] = std::stod(avg_rating);
  new_doc["num_rating"] = num_rating;
  new_doc["casts"] = json::array();
  for (auto &cast : casts) {
      json c;
      c["cast_id"] = cast.cast_id;
      c["cast_info_id"] = cast.cast_info_id;
      c["character"] = cast.character;
      new_doc["casts"].push_back(c);
  }
  new_doc["thumbnail_ids"] = thumbnail_ids;
  new_doc["photo_ids"] = photo_ids;
  new_doc["video_ids"] = video_ids;

  std::string url = _couchdb_url + movie_id;
  try {
    couchdb_do_http_put(url, new_doc.dump());
    LOG(info) << "wrote movie (movie_id=" << movie_id << ") to CouchDB";
  } catch (const std::exception &e) {
      LOG(debug) << "failed to write movie (movie_id=" << movie_id << ") to CouchDB: " << e.what();
      ServiceException se;
      se.errorCode = ErrorCode::SE_COUCHDB_ERROR;
      se.message = e.what();
      throw se;
  }

  span->Finish();
}

void MovieInfoHandler::ReadMovieInfo(
    MovieInfo &_return,
    int64_t req_id,
    const std::string &movie_id,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadMovieInfo",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

   LOG(info) << "REQUEST to read movie info (movie_id=" << movie_id << ")";
  
  memcached_return_t memcached_rc;
  memcached_st *memcached_client = memcached_pool_pop(
      _memcached_client_pool, true, &memcached_rc);
  if (!memcached_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  size_t movie_info_mmc_size;
  uint32_t memcached_flags;
  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetMovieInfo", { opentracing::ChildOf(&span->context()) });
  char *movie_info_mmc = memcached_get(
      memcached_client,
      movie_id.c_str(),
      movie_id.length(),
      &movie_info_mmc_size,
      &memcached_flags,
      &memcached_rc);
  if (!movie_info_mmc && memcached_rc != MEMCACHED_NOTFOUND) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = memcached_strerror(memcached_client, memcached_rc);
    memcached_pool_push(_memcached_client_pool, memcached_client);
    throw se;
  }
  memcached_pool_push(_memcached_client_pool, memcached_client);
  get_span->Finish();

  if (movie_info_mmc) {
    LOG(debug) << "Get movie-info " << movie_id << " cache hit from Memcached";
    json movie_info_json = json::parse(std::string(
        movie_info_mmc, movie_info_mmc + movie_info_mmc_size));
    _return.movie_id = movie_info_json["movie_id"];
    _return.title = movie_info_json["title"];
    _return.avg_rating = movie_info_json["avg_rating"];
    _return.num_rating = movie_info_json["num_rating"];
    _return.plot_id = movie_info_json["plot_id"];

    LOG(debug) << "movie_id=" << _return.movie_id << ", title=" << _return.title << ", plot_id=" << _return.plot_id;

    for (auto &item : movie_info_json["photo_ids"]) {
      _return.photo_ids.emplace_back(item);
    }
    for (auto &item : movie_info_json["video_ids"]) {
      _return.video_ids.emplace_back(item);
    }
    for (auto &item : movie_info_json["thumbnail_ids"]) {
      _return.thumbnail_ids.emplace_back(item);
    }
    for (auto &item : movie_info_json["casts"]) {
      Cast new_cast;
      new_cast.cast_id = item["cast_id"];
      new_cast.cast_info_id = item["cast_info_id"];
      new_cast.character = item["character"];
      _return.casts.emplace_back(new_cast);
    }
    free(movie_info_mmc);
  } else {
    // If not cached in memcached

    std::string url = _couchdb_url + movie_id;
    std::string response;
    try {
      response = couchdb_do_http_get(url);
      LOG(info) << "read movie (movie_id=" << movie_id << ") from CouchDB";
    } catch (const std::exception &e) {
      LOG(debug) << "failed to read movie (movie_id=" << movie_id << ") from CouchDB: " << e.what();
      ServiceException se;
      se.errorCode = ErrorCode::SE_COUCHDB_ERROR;
      se.message = e.what();
      throw se;
    }

    auto movie_info_json = json::parse(response);
    _return.movie_id = movie_info_json["movie_id"];
    _return.title = movie_info_json["title"];
    _return.avg_rating = movie_info_json["avg_rating"];
    _return.num_rating = movie_info_json["num_rating"];
    _return.plot_id = movie_info_json["plot_id"];

    for (auto &item : movie_info_json["photo_ids"]) {
      _return.photo_ids.push_back(item);
    }
    for (auto &item : movie_info_json["video_ids"]) {
      _return.video_ids.push_back(item);
    }
    for (auto &item : movie_info_json["thumbnail_ids"]) {
      _return.thumbnail_ids.push_back(item);
    }

    for (auto &item : movie_info_json["casts"]) {
      Cast c;
      c.cast_id = item["cast_id"];
      c.cast_info_id = item["cast_info_id"];
      c.character = item["character"];
      _return.casts.push_back(c);
    }

    // upload movie-info to memcached
    memcached_client = memcached_pool_pop(
        _memcached_client_pool, true, &memcached_rc);
    if (!memcached_client) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Failed to pop a client from memcached pool";
      throw se;
    }
    auto set_span = opentracing::Tracer::Global()->StartSpan(
      "MmcSetMovieInfo", { opentracing::ChildOf(&span->context()) });

    std::string movie_info_str = movie_info_json.dump();

    memcached_rc = memcached_set(
      memcached_client,
      movie_id.c_str(),
      movie_id.length(),
      movie_info_str.data(),
      movie_info_str.size(),
      static_cast<time_t>(0),
      static_cast<uint32_t>(0));
    if (memcached_rc != MEMCACHED_SUCCESS) {
      LOG(warning) << "Failed to set movie_info to Memcached: "
                    << memcached_strerror(memcached_client, memcached_rc);
    }
    set_span->Finish();
    memcached_pool_push(_memcached_client_pool, memcached_client);
  }

  LOG(info) << "OK (movie_id=" << movie_id << ")";
  span->Finish();
}

void MovieInfoHandler::UpdateRating(
    int64_t req_id, const std::string& movie_id,
    int32_t sum_uncommitted_rating, int32_t num_uncommitted_rating,
    const std::map<std::string, std::string> & carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "UpdateRating",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  bson_t *query = bson_new();
  BSON_APPEND_UTF8(query, "movie_id", movie_id.c_str());

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(
      _mongodb_client_pool);
  if (!mongodb_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to pop a client from MongoDB pool";
    throw se;
  }
  auto collection = mongoc_client_get_collection(
      mongodb_client, "social-graph", "social-graph");
  if (!collection) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MONGODB_ERROR;
    se.message = "Failed to create collection social_graph from MongoDB";
    mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
    throw se;
  }
  auto find_span = opentracing::Tracer::Global()->StartSpan(
      "MongoFindMovieInfo", {opentracing::ChildOf(&span->context())});
  mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(
      collection, query, nullptr, nullptr);
  const bson_t *doc;
  bool found = mongoc_cursor_next(cursor, &doc);
  if (found) {
    bson_iter_t iter_0;
    bson_iter_t iter_1;
    bson_iter_init(&iter_0, doc);
    bson_iter_init(&iter_1, doc);
    double avg_rating;
    int32_t num_rating;
    if (bson_iter_init_find(&iter_0, doc, "avg_rating") &&
        bson_iter_init_find(&iter_1, doc, "num_rating")) {
      avg_rating = bson_iter_value(&iter_0)->value.v_double;
      num_rating = bson_iter_value(&iter_1)->value.v_int32;

      avg_rating = (avg_rating * num_rating + sum_uncommitted_rating) /
          (num_rating + num_uncommitted_rating);
      num_rating += num_uncommitted_rating;

      bson_t *update = BCON_NEW(
          "$set", "{",
          "avg_rating", BCON_DOUBLE(avg_rating),
          "num_rating", BCON_INT32(num_rating), "}");
      bson_error_t error;
      bson_t reply;
      auto update_span = opentracing::Tracer::Global()->StartSpan(
          "MongoUpdateRating", {opentracing::ChildOf(&span->context())});
      bool updated = mongoc_collection_find_and_modify(
          collection,
          query,
          nullptr,
          update,
          nullptr,
          false,
          false,
          true,
          &reply,
          &error);
      if (!updated) {
        LOG(error) << "Failed to update rating for movie " << movie_id
                   << " to MongoDB: " << error.message;
        ServiceException se;
        se.errorCode = ErrorCode::SE_MONGODB_ERROR;
        se.message = "Failed to update rating for movie " + movie_id +
            " to MongoDB: " + error.message;
        bson_destroy(&reply);
        bson_destroy(update);
        mongoc_collection_destroy(collection);
        mongoc_client_pool_push(_mongodb_client_pool, mongodb_client);
        throw se;
      }
      update_span->Finish();
    }
  }

  auto delete_span = opentracing::Tracer::Global()->StartSpan(
      "MmcDelete", {opentracing::ChildOf(&span->context())});
  memcached_return_t memcached_rc;
  memcached_st *memcached_client = memcached_pool_pop(
      _memcached_client_pool, true, &memcached_rc);
  if (!memcached_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }
  memcached_delete(memcached_client, movie_id.c_str(), movie_id.length(), 0);
  memcached_pool_push(_memcached_client_pool, memcached_client);
  delete_span->Finish();

  span->Finish();
}

} // namespace media_service

#endif //MEDIA_MICROSERVICES_SRC_MOVIEINFOSERVICE_MOVIEINFOHANDLER_H_
