#ifndef MEDIA_SERVICE_MICROSERVICES_SRC_UTILS_COUCHDB_H_
#define MEDIA_SERVICE_MICROSERVICES_SRC_UTILS_COUCHDB_H_

#include <curl/curl.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace media_service {
  static size_t _couchdb_curl_write_cb(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t total = size * nmemb;
    static_cast<std::string*>(userp)->append((char*)contents, total);
    return total;
  }

  static std::string couchdb_do_http_get(const std::string &url) {
    CURL *curl = curl_easy_init();
    if (!curl) throw std::runtime_error("curl_easy_init failed");

    std::string resp;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, _couchdb_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);

    CURLcode rc = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    if (rc != CURLE_OK) {
      throw std::runtime_error(std::string("http get failed: ") + curl_easy_strerror(rc));
    }
    return resp;
  }

  static std::string couchdb_do_http_put(const std::string &url, const std::string &json_body) {
    CURL *curl = curl_easy_init();
    if (!curl) throw std::runtime_error("curl_easy_init failed");

    std::string resp;
    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "content-type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_body.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, _couchdb_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

    CURLcode rc = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
      throw std::runtime_error(std::string("http put failed: ") + curl_easy_strerror(rc));
    }
    return resp;
  }

  static std::vector<int64_t> couchdb_load_review_ids(const std::string& base, const std::string& movie_id) {
    auto body = couchdb_do_http_get(base + movie_id);
    auto j = json::parse(body);
    std::vector<int64_t> ids;
    if (j.contains("reviews") && j["reviews"].is_array()) {
      for (auto& r : j["reviews"]) {
        // you were storing {review_id, timestamp}; keep same shape
        ids.emplace_back(r.value("review_id", int64_t{0}));
      }
    }
    return ids;
  }

  static void couchdb_append_review(const std::string& base, const std::string& movie_id, int64_t review_id, int64_t timestamp) {
    auto url = base + movie_id;

    json doc;
    std::string body;
    try {
      body = couchdb_do_http_get(url);
      doc = json::parse(body);
      if (!doc.contains("reviews") || !doc["reviews"].is_array()) doc["reviews"] = json::array();
      doc["reviews"].push_back(json{{"review_id", review_id},{"timestamp", timestamp}});

    } catch (const std::exception& e) {
      // create new document
      doc["_id"] = movie_id;
      doc["movie_id"] = movie_id;
      doc["reviews"] = json::array({ json{{"review_id", review_id},{"timestamp", timestamp}} });
    }

    // first put
    couchdb_do_http_put(url, doc.dump());
  }


} // namespace media_service

#endif //MEDIA_SERVICE_MICROSERVICES_SRC_UTILS_COUCHDB_H_
