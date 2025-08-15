add_executable(
    MovieIdService
    MovieIdService.cpp
    ${THRIFT_GEN_CPP_DIR}/MovieIdService.cpp
    ${THRIFT_GEN_CPP_DIR}/RatingService.cpp
    ${THRIFT_GEN_CPP_DIR}/ComposeReviewService.cpp
    ${THRIFT_GEN_CPP_DIR}/media_service_types.cpp
)

set(AWS_SDK_LIB_PATH /usr/local/lib)
set(AWS_SDK_INCLUDE_PATH /usr/local/include)
find_package(AWSSDK REQUIRED COMPONENTS dynamodb)

target_include_directories(
    MovieIdService PRIVATE
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${CURL_INCLUDE_DIRS}
    ${MONGOC_INCLUDE_DIRS}
    ${AWS_SDK_INCLUDE_PATH}
    ${PQXX_INCLUDE_DIRS}
    /usr/local/include/jaegertracing
)

target_link_libraries(
    MovieIdService
    ${AWSSDK_LINK_LIBRARIES}
    ${CMAKE_DL_LIBS}
    ${CURL_LIBRARIES}
    ${PQXX_LIBRARIES}
    mongoc-1.0
    bson-1.0
    ${LIBMEMCACHED_LIBRARIES}
    nlohmann_json::nlohmann_json
    ${THRIFT_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

target_compile_definitions (
    MovieIdService PRIVATE
    "${MONGOC_DEFINITIONS}"
)

install(TARGETS MovieIdService DESTINATION ./)
