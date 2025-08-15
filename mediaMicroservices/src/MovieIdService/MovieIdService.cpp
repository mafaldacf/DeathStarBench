#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <signal.h>
#include <cstdlib>

#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "MovieIdHandler.h"

using json = nlohmann::json;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TServerSocket;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace media_service;

void sigintHandler(int sig) {
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "movie-id-service");

  Aws::SDKOptions options;
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration config;
  std::string aws_region = std::getenv("AWS_REGION");
  std::string dynamo_table_name = "dsb-movie-id";
  config.region = aws_region;
  Aws::DynamoDB::DynamoDBClient dynamo_client(config);

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    Aws::ShutdownAPI(options);
    exit(EXIT_FAILURE);
  }

  int port = config_json["movie-id-service"]["port"];
  std::string compose_addr = config_json["compose-review-service"]["addr"];
  int compose_port = config_json["compose-review-service"]["port"];
  std::string rating_addr = config_json["rating-service"]["addr"];
  int rating_port = config_json["rating-service"]["port"];

  std::string couchdb_address = std::getenv("COUCHDB_ADDRESS");
  std::string couchdb_url = "http://admin:admin@" + couchdb_address + "/movieid/";

  std::string postgresql_address = std::getenv("POSTGRESQL_ADDRESS");
  std::string postgres_url = "postgresql://admin:admin@" + postgresql_address + "/mediamicroservices";

  const char* backend_env = std::getenv("MOVIEID_BACKEND");
  if (!backend_env) {
      throw std::runtime_error("MOVIEID_BACKEND environment variable not set");
  }
  std::string backend_str(backend_env);
  MovieIdHandler::BackendType backend_type;
  if (backend_str == "DYNAMODB") {
    backend_type = MovieIdHandler::BackendType::DynamoDB;
  } else if (backend_str == "COUCHDB") {
      backend_type = MovieIdHandler::BackendType::CouchDB;
  } else if (backend_str == "POSTGRESQL") {
    backend_type = MovieIdHandler::BackendType::PostgreSQL;
  } else {
    throw std::runtime_error("unknown MOVIEID_BACKEND value: " + backend_str);
  }

  memcached_pool_st *memcached_client_pool =
      init_memcached_client_pool(config_json, "movie-id", 32, 128);
  mongoc_client_pool_t* mongodb_client_pool =
      init_mongodb_client_pool(config_json, "movie-id", 128);

  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  ClientPool<ThriftClient<ComposeReviewServiceClient>> compose_client_pool(
      "compose-review-client", compose_addr, compose_port, 0, 128, 1000);
  ClientPool<ThriftClient<RatingServiceClient>> rating_client_pool(
      "rating-client", rating_addr, rating_port, 0, 128, 1000);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "movie-id", "movie_id", true);
    r = CreateIndex(mongodb_client, "movie-id", "title", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  TThreadedServer server(
      std::make_shared<MovieIdServiceProcessor>(
      std::make_shared<MovieIdHandler>(
              memcached_client_pool, mongodb_client_pool,
              &compose_client_pool, &rating_client_pool,
              &dynamo_client, aws_region, dynamo_table_name,
              couchdb_url, postgres_url,
              backend_type)),
      std::make_shared<TServerSocket>("0.0.0.0", port),
      std::make_shared<TFramedTransportFactory>(),
      std::make_shared<TBinaryProtocolFactory>()
  );
  std::cout << "Starting the movie-id-service server ..." << std::endl;
  server.serve();

   Aws::ShutdownAPI(options);
}


