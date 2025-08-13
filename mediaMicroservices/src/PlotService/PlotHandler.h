#ifndef MEDIA_MICROSERVICES_PLOTHANDLER_H
#define MEDIA_MICROSERVICES_PLOTHANDLER_H

#include <iostream>
#include <string>

#include <libmemcached/memcached.h>
#include <libmemcached/util.h>
#include <mongoc.h>
#include <bson/bson.h>
#include <nlohmann/json.hpp>
#include "../utils_couchdb.h"

#include "../../gen-cpp/PlotService.h"
#include "../logger.h"
#include "../tracing.h"

namespace media_service {

  using json = nlohmann::json;
  
class PlotHandler : public PlotServiceIf {
 public:
  PlotHandler(
      memcached_pool_st *,
      mongoc_client_pool_t *,
      std::string);
  ~PlotHandler() override = default;

  void WritePlot(int64_t req_id, int64_t plot_id, const std::string& plot,
      const std::map<std::string, std::string> & carrier) override;
  void ReadPlot(std::string& _return, int64_t req_id, int64_t plot_id,
      const std::map<std::string, std::string> & carrier) override;

 private:
  memcached_pool_st *_memcached_client_pool;
  mongoc_client_pool_t *_mongodb_client_pool;
  std::string _couchdb_url;
};

PlotHandler::PlotHandler(
    memcached_pool_st *memcached_client_pool,
    mongoc_client_pool_t *mongodb_client_pool,
    std::string couchdb_url) {
  _memcached_client_pool = memcached_client_pool;
  _mongodb_client_pool = mongodb_client_pool;
  _couchdb_url = couchdb_url;
}

void PlotHandler::ReadPlot(
    std::string &_return,
    int64_t req_id,
    int64_t plot_id,
    const std::map<std::string, std::string> & carrier) {

  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "ReadPlot",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  memcached_return_t memcached_rc;
  memcached_st *memcached_client = memcached_pool_pop(
      _memcached_client_pool, true, &memcached_rc);
  if (!memcached_client) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = "Failed to pop a client from memcached pool";
    throw se;
  }

  size_t plot_size;
  uint32_t memcached_flags;

  // Look for the movie id from memcached
  auto get_span = opentracing::Tracer::Global()->StartSpan(
      "MmcGetPlot", { opentracing::ChildOf(&span->context()) });
  auto plot_id_str = std::to_string(plot_id);

  char* plot_mmc = memcached_get(
      memcached_client,
      plot_id_str.c_str(),
      plot_id_str.length(),
      &plot_size,
      &memcached_flags,
      &memcached_rc);
  if (!plot_mmc && memcached_rc != MEMCACHED_NOTFOUND) {
    ServiceException se;
    se.errorCode = ErrorCode::SE_MEMCACHED_ERROR;
    se.message = memcached_strerror(memcached_client, memcached_rc);
    memcached_pool_push(_memcached_client_pool, memcached_client);
    throw se;
  }
  get_span->Finish();
  memcached_pool_push(_memcached_client_pool, memcached_client);

  // If cached in memcached
  if (plot_mmc) {
    LOG(debug) << "Get plot " << plot_mmc
        << " cache hit from Memcached";
    _return = std::string(plot_mmc);
    free(plot_mmc);
  } else {
    const std::string url = _couchdb_url + plot_id_str;
    std::string response;
    try {
      response = couchdb_get(url);
      LOG(info) << "read plot (plot_id=" << plot_id_str << ") from CouchDB";
    } catch (const std::exception &e) {
      LOG(debug) << "failed to read plot (plot_id=" << plot_id_str << ") from CouchDB: " << e.what();
      ServiceException se;
      se.errorCode = ErrorCode::SE_COUCHDB_ERROR;
      se.message = e.what();
      throw se;
    }

    auto j = json::parse(response);
    if (!j.contains("plot") || !j["plot"].is_string()) {
      ServiceException se;
      se.errorCode = ErrorCode::SE_THRIFT_HANDLER_ERROR;
      se.message = "plot field missing in CouchDB doc id=" + plot_id_str;
      throw se;
    }
    _return = j["plot"].get<std::string>();


    // Upload the plot to memcached
    auto set_span = opentracing::Tracer::Global()->StartSpan(
        "MmcSetPlot", { opentracing::ChildOf(&span->context()) });
    memcached_rc = memcached_set(
        memcached_client,
        plot_id_str.c_str(),
        plot_id_str.length(),
        _return.c_str(),
        _return.length(),
        static_cast<time_t>(0),
        static_cast<uint32_t>(0)
    );
    set_span->Finish();

    if (memcached_rc != MEMCACHED_SUCCESS) {
      LOG(warning) << "Failed to set plot to Memcached: "
          << memcached_strerror(memcached_client, memcached_rc);
    }
    memcached_pool_push(_memcached_client_pool, memcached_client);
  }
  span->Finish();
}

void PlotHandler::WritePlot(
    int64_t req_id,
    int64_t plot_id,
    const std::string &plot,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "WritePlot",
      { opentracing::ChildOf(parent_span->get()) });
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  LOG(info) << "REQUEST to write plot (plot_id=" << plot_id << ", plot=" << plot.c_str() << ")";

  const std::string plot_id_str = std::to_string(plot_id);
  json doc;
  doc["_id"] = plot_id_str;
  doc["plot_id"] = plot_id;
  doc["plot"] = plot;

  const std::string url = _couchdb_url + plot_id_str;
  try {
    couchdb_put(url, doc.dump());
    LOG(info) << "wrote plot (plot_id=" << plot_id_str << ") to CouchDB";
  } catch (const std::exception &e) {
    LOG(debug) << "failed to write plot (plot_id=" << plot_id_str << ") to CouchDB: " << e.what();
    ServiceException se;
    se.errorCode = ErrorCode::SE_COUCHDB_ERROR;
    se.message = e.what();
    throw se;
  }

  LOG(info) << "OK (plot_id=" << plot_id << ", plot=" << plot.c_str() << ")";
  span->Finish();
}

} // namespace media_service

#endif //MEDIA_MICROSERVICES_PLOTHANDLER_H
