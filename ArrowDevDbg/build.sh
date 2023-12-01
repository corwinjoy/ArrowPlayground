#/bin/bash


CONTAINER_TAG=arrow-debug:latest
docker build --build-arg UID=$(id -u) -t $CONTAINER_TAG -f Dockerfile .
# docker run -it -v ./src:/src $CONTAINER_TAG /bin/bash
CONTAINER_ID=$(docker run -dit -v ./src:/src $CONTAINER_TAG)
docker exec -t $CONTAINER_ID /bin/bash -c '/bin/bash /install_arrow.sh'
docker commit $CONTAINER_ID $CONTAINER_TAG
docker stop $CONTAINER_ID



: '
# Example run and profile
docker run --privileged -it -v ./src:/src -v /home/cjoy/src/ArrowPlayground:/tmp   arrow-debug:latest bash
export PERF_DIR=/tmp/ColumnReadingPerf/cmake-build-debug-docker_dbarrow
/usr/bin/perf record --freq=900 --call-graph dwarf -q -o $PERF_DIR/arrow_perf.data $PERF_DIR/arrow_benchmarks
perf report -i $PERF_DIR/arrow_perf.data
'
