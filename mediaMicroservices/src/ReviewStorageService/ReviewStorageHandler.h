#ifndef MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H
#define MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H

#include <iostream>
#include <string>
#include <future>

#include <mongoc.h>
#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <bson/bson.h>
#include <nlohmann/json.hpp>
#include "../utils_couchdb.h"

#include "../../gen-cpp/ReviewStorageService.h"
#include "../logger.h"
#include "../tracing.h"

using json = json;

namespace media_service {

class ReviewStorageHandler : public ReviewStorageServiceIf{
 public:
  ReviewStorageHandler(memcached_pool_st *, mongoc_client_pool_t *, std::string);
  ~ReviewStorageHandler() override = default;
  void StoreReview(int64_t, const Review &, 
      const std::map<std::string, std::string> &) override;
  void ReadReviews(std::vector<Review> &, int64_t, const std::vector<int64_t> &,
                   const std::map<std::string, std::string> &) override;
  
 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  std::string _couchdb_url;
};

ReviewStorageHandler::ReviewStorageHandler(
    memcached_pool_st *memcached_pool,
    mongoc_client_pool_t *mongodb_pool,
    std::string couchdb_url) {
  _memcached_client_pool = memcached_pool;
  _mongodb_client_pool = mongodb_pool;
  _couchdb_url = couchdb_url;
}

void ReviewStorageHandler::StoreReview(
    int64_t req_id, 
    const Review &review,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "StoreReview",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "request to store review (movie_id=" << review.movie_id.c_str() << ", review_id=" << review.review_id << ")";


  json new_doc;
  const std::string doc_id = std::to_string(review.review_id);
  new_doc["_id"]      = doc_id;
  new_doc["review_id"]= review.review_id;
  new_doc["timestamp"]= review.timestamp;
  new_doc["user_id"]  = review.user_id;
  new_doc["movie_id"] = review.movie_id;
  new_doc["text"]     = review.text;
  new_doc["rating"]   = review.rating;
  new_doc["req_id"]   = review.req_id;
  std::string url = _couchdb_url + doc_id;

  auto insert_span = opentracing::Tracer::Global()->StartSpan(
      "CouchDBPutReview", { opentracing::ChildOf(&span->context()) });

  try {
    couchdb_put(url, new_doc.dump());
    LOG(info) << "wrote review (review_id=" << review.review_id << ", movie_id=" << review.movie_id << ") to couchdb";
  } catch (const std::exception &e) {
    LOG(debug) << "failed to insert review to couchdb: " << e.what();
    ServiceException se;
    se.errorCode = ErrorCode::SE_COUCHDB_ERROR;
    se.message = e.what();
    insert_span->Finish();
    throw se;
  }
  insert_span->Finish();

  LOG(info) << "OK! (movie_id=" << review.movie_id.c_str() << ", review_id=" << review.review_id << ")";

  span->Finish();
}
void ReviewStorageHandler::ReadReviews(
    std::vector<Review> & _return,
    int64_t req_id,
    const std::vector<int64_t> &review_ids,
    const std::map<std::string, std::string> &carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadReviews",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "REQUEST to read reviews (number of review IDs=" << review_ids.size() << ")";

  if (review_ids.empty()) {
    LOG(info) << "OK (number of review IDs=" << review_ids.size() << ")";
    return;
  }

  std::set<int64_t> review_ids_not_cached(review_ids.begin(), review_ids.end());
  if (review_ids_not_cached.size() != review_ids.size()) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "Post_ids are duplicated";
    throw se;
  }
  std::map<int64_t, Review> return_map;
  memcached_return_t memcached_rc;
  auto memcached_client = memcached_pool_pop(
      _memcached_client_pool, true, &memcached_rc);
  if (!memcached_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  char** keys;
  size_t *key_sizes;
  keys = new char* [review_ids.size()];
  key_sizes = new size_t [review_ids.size()];
  int idx = 0;
  for (auto &review_id : review_ids) {
    std::string key_str = std::to_string(review_id);
    keys[idx] = new char [key_str.length() + 1];
    strcpy(keys[idx], key_str.c_str());
    key_sizes[idx] = key_str.length();
    idx++;
  }
  memcached_rc = memcached_mget(
      memcached_client, keys, key_sizes, review_ids.size());
  if (memcached_rc != MEMCACHED_SUCCESS) {
    LOG(error) << "Cannot get review-ids of request " << req_id << ": "
               << memcached_strerror(memcached_client, memcached_rc);
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = memcached_strerror(memcached_client, memcached_rc);
    memcached_pool_push(_memcached_client_pool, memcached_client);
    throw se;
  }

  char return_key[MEMCACHED_MAX_KEY];
  size_t return_key_length;
  char *return_value;
  size_t return_value_length;
  uint32_t flags;
  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MemcachedMget", { opentracing::ChildOf(&span->context()) });

  while (true) {
    return_value =
        memcached_fetch(memcached_client, return_key, &return_key_length,
                        &return_value_length, &flags, &memcached_rc);
    if (return_value == nullptr) {
      LOG(debug) << "Memcached mget finished";
      break;
    }
    if (memcached_rc != MEMCACHED_SUCCESS) {
      free(return_value);
      memcached_quit(memcached_client);
      memcached_pool_push(_memcached_client_pool, memcached_client);
      LOG(error) << "Cannot get reviews of request " << req_id;
      ServiceException se;
      se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
      se.message = "Cannot get reviews of request " + std::to_string(req_id);
      throw se;
    }
    Review new_review;
    json review_json = json::parse(std::string(
        return_value, return_value + return_value_length));
    new_review.req_id = review_json["req_id"];
    new_review.user_id = review_json["user_id"];
    new_review.movie_id = review_json["movie_id"];
    new_review.text = review_json["text"];
    new_review.rating = review_json["rating"];
    new_review.timestamp = review_json["timestamp"];
    new_review.review_id = review_json["review_id"];
    return_map.insert(std::make_pair(new_review.review_id, new_review));
    review_ids_not_cached.erase(new_review.review_id);
    free(return_value);
    LOG(debug) << "Review: " << new_review.review_id << " found in memcached";
  }
  get_span->Finish();
  memcached_quit(memcached_client);
  memcached_pool_push(_memcached_client_pool, memcached_client);
  for (int i = 0; i < review_ids.size(); ++i) {
    delete keys[i];
  }
  delete[] keys;
  delete[] key_sizes;

  std::vector<std::future<void>> set_futures;
  std::map<int64_t, std::string> review_json_map;
  
  // Find the rest in MongoDB
  if (!review_ids_not_cached.empty()) {
    json body;
    body["keys"] = json::array();
    for (const auto &rid : review_ids_not_cached) {
      body["keys"].push_back(std::to_string(rid));
    }

    const std::string url = _couchdb_url + "_all_docs?include_docs=true";

    auto find_span = opentracing::Tracer::Global()->StartSpan(
        "CouchBulkGetReviews", { opentracing::ChildOf(&span->context()) });

    std::string resp;
    try {
      resp = couchdb_post(url, body.dump());
    } catch (const std::exception &e) {
      find_span->Finish();
      LOG(debug) << "failed to bulk fetch reviews from couchdb: " << e.what();
      ServiceException se; 
      se.errorCode = ErrorCode::SE_COUCHDB_ERROR; 
      se.message = e.what(); 
      throw se;
    }
    find_span->Finish();

    json j = json::parse(resp);
    if (j.contains("rows") && j["rows"].is_array()) {
      for (auto &row : j["rows"]) {
        if (!row.contains("doc") || row["doc"].is_null()) continue;
        auto &d = row["doc"];

        Review new_review;
        new_review.req_id   = d.value("req_id",   static_cast<int64_t>(0));
        new_review.user_id  = d.value("user_id",  static_cast<int64_t>(0));
        new_review.movie_id = d.value("movie_id", std::string{});
        new_review.text     = d.value("text",     std::string{});
        new_review.rating   = d.value("rating",   0);
        new_review.timestamp= d.value("timestamp",static_cast<int64_t>(0));

        if (d.contains("review_id")) {
          if (d["review_id"].is_number_integer())
            new_review.review_id = d["review_id"].get<int64_t>();
          else if (d["review_id"].is_string())
            new_review.review_id = std::stoll(d["review_id"].get<std::string>());
        } else if (d.contains("_id") && d["_id"].is_string()) {
          // fallback to doc _id
          try { new_review.review_id = std::stoll(d["_id"].get<std::string>()); } catch (...) {}
        }

        std::string review_json_str = d.dump();

        review_json_map.insert({ new_review.review_id, review_json_str });
        return_map.insert({ new_review.review_id, new_review });
      }
    }

    set_futures.emplace_back(std::async(std::launch::async, [&]() {
      memcached_return_t _rc;
      auto *_memcached_client = memcached_pool_pop(_memcached_client_pool, true, &_rc);
      if (!_memcached_client) {
        LOG(error) << "failed to pop a client from memcached pool";
        ServiceException se; se.errorCode = ErrorCode::SE_MEMCACHED_ERROR; se.message = "failed to pop a client from memcached pool"; throw se;
      }
      auto set_span = opentracing::Tracer::Global()->StartSpan(
          "MmcSetReview", { opentracing::ChildOf(&span->context()) });

      for (auto &it : review_json_map) {
        std::string id_str = std::to_string(it.first);
        const std::string &val = it.second;
        _rc = memcached_set(
            _memcached_client,
            id_str.c_str(),
            id_str.length(),
            val.data(),
            val.size(),
            static_cast<time_t>(0),
            static_cast<uint32_t>(0));
        if (_rc != MEMCACHED_SUCCESS) {
          LOG(warning) << "failed to set review " << id_str << " to memcached: "
                      << memcached_strerror(_memcached_client, _rc);
        }
      }
      memcached_pool_push(_memcached_client_pool, _memcached_client);
      set_span->Finish();
    }));
  }

  if (return_map.size() != review_ids.size()) {
    try {
      for (auto &it : set_futures) { it.get(); }
    } catch (...) {
      LOG(warning) << "Failed to set reviews to memcached";
    }
    LOG(warning) << "review storage service: return set incomplete";
    /* ServiceException se;
    se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
    se.message = "review storage service: return set incomplete";
    throw se; */
  }

  for (auto &review_id : review_ids) {
    LOG(info) << "got review ID" << review_id;
    _return.emplace_back(return_map[review_id]);
  }

  try {
    for (auto &it : set_futures) { it.get(); }
  } catch (...) {
    LOG(warning) << "Failed to set reviews to memcached";
  }

  LOG(info) << "OK (number of review IDs=" << review_ids.size() << ")";
  
}

} // namespace media_service


#endif //MEDIA_MICROSERVICES_REVIEWSTOREHANDLER_H
