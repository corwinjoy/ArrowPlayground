# Use the specified base image
FROM apache/arrow-dev:amd64-ubuntu-22.04-python-3
# Set environment variables
ENV PYARROW_WITH_PARQUET=1 \
    PYARROW_WITH_DATASET=1 \
    PYARROW_WITH_PARQUET_ENCRYPTION=1 \
	PYARROW_BUILD_TYPE=Debug

# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y \
    autoconf \
    automake \
    binutils \
    build-essential \
    clang \
    cmake \
    dos2unix \
    g++ \
    gcc \
    gdb \
    git \
    htop \
    iputils-ping \
    libtool \
    linux-tools-generic \
    locales-all \
    make \
    mc \
    nano \
    net-tools \
    ninja-build \
    python3-dbg \
    python3-dev \
    rsync \
    strace \
    tar \
    valgrind \
    wget \
#Clean apt cache
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* \
	&& echo

# Clone the specified Arrow repository and checkout the desired branch
WORKDIR /src
RUN git clone https://github.com/apache/arrow.git && \
    cd arrow 

# Build Arrow C++
WORKDIR /src/arrow/cpp
RUN mkdir build && cd build && \
    cmake .. \
	-DARROW_PARQUET=ON \
    -DPARQUET_REQUIRE_ENCRYPTION=ON \
    -DARROW_DATASET=ON \
    -DARROW_ACERO=ON \
    -DARROW_ALTIVEC=ON \
    -DARROW_AZURE=OFF \
    -DARROW_BOOST_USE_SHARED=ON \
    -DARROW_BROTLI_USE_SHARED=ON \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_CONFIG_SUMMARY_JSON=ON \
    -DARROW_BUILD_INTEGRATION=ON \
	-DCMAKE_BUILD_TYPE="Debug" \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_TESTS=ON \
    -DARROW_BZ2_USE_SHARED=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DEFINE_OPTIONS=ON \
    -DARROW_DEPENDENCY_SOURCE=AUTO \
    -DARROW_DEPENDENCY_USE_SHARED=ON \
    -DARROW_ENABLE_TIMING_TESTS=ON \
    -DARROW_EXTRA_ERROR_CONTEXT=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_GFLAGS_USE_SHARED=ON \
    -DARROW_GGDB_DEBUG=ON \
    -DARROW_GRPC_USE_SHARED=ON \
    -DARROW_IPC=ON \
    -DARROW_JEMALLOC=ON \
    -DARROW_JEMALLOC_USE_SHARED=ON \
    -DARROW_JSON=ON \
    -DARROW_LZ4_USE_SHARED=ON \
    -DARROW_MIMALLOC=ON \
    -DARROW_OPENSSL_USE_SHARED=ON \
    -DARROW_ORC=ON \
    -DARROW_POSITION_INDEPENDENT_CODE=ON \
    -DARROW_PROTOBUF_USE_SHARED=ON \
    -DARROW_RE2_LINKAGE=static \
    -DARROW_RUNTIME_SIMD_LEVEL=MAX \
    -DARROW_S3=OFF \
    -DARROW_SIMD_LEVEL=DEFAULT \
    -DARROW_SNAPPY_USE_SHARED=ON \
    -DARROW_SUBSTRAIT=ON \
    -DARROW_TEST_LINKAGE=shared \
    -DARROW_THRIFT_USE_SHARED=ON \
    -DARROW_USE_CCACHE=ON \
    -DARROW_UTF8PROC_USE_SHARED=ON \
    -DARROW_WITH_BACKTRACE=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_RE2=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_UTF8PROC=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_ZSTD_USE_SHARED=ON \
    -GNinja && \
    ninja install

# Remove any previous version of Arrow Python package
RUN find /usr/local/lib/ -depth -name pyarrow -type d -print -exec rm -r {} \;

# Build Arrow Python
WORKDIR /src/arrow/python
RUN python setup.py build_ext --inplace --with-parquet-encryption --with-dataset --with-parquet --build-type=Debug && \
    python setup.py install

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
RUN pip3 install jupyterlab polars parquet-tools build

WORKDIR /src
RUN git clone https://github.com/adamreeve/parquet-inspect.git && \
    cd parquet-inspect && \
	python -m build  && \
	pip install --editable .
	
# Add a builder user to handle file sharing in Linux
ARG UID=1000
RUN useradd -m -u ${UID} -s /bin/bash builder
USER builder

# Command to keep the container running
CMD ["bash"]
