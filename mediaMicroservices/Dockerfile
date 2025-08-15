FROM yg397/thrift-microservice-deps:xenial

RUN apt-get update && apt-get install -y \
    curl \
    libcurl4-openssl-dev \
    zlib1g-dev \ 
    libssl-dev \ 
    libpulse-dev \
    uuid-dev \
    libpqxx-dev \
    libpq-dev

# Upgrade CMake
RUN wget https://cmake.org/files/v3.22/cmake-3.22.1.tar.gz
RUN tar -xzf cmake-3.22.1.tar.gz
RUN cd cmake-3.22.1 && ./bootstrap
RUN cd cmake-3.22.1 && make -j$(nproc)
RUN cd cmake-3.22.1 && make install

# Install AWS SDK
RUN git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git
RUN cd aws-sdk-cpp && git submodule update --init --recursive

RUN cd aws-sdk-cpp && mkdir build && cd build && cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_ONLY="dynamodb" \
    -DENABLE_TESTING=OFF

RUN cd aws-sdk-cpp/build && make -j$(nproc)
RUN cd aws-sdk-cpp/build && make install
RUN rm -rf aws-sdk-cpp

COPY ./ /media-microservices

RUN mkdir -p /media-microservices/build
RUN cd /media-microservices/build && cmake ..
RUN cd /media-microservices/build && make VERBOSE=1
RUN cd /media-microservices/build && make install

WORKDIR /media-microservices
