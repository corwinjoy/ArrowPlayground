#/bin/bash

docker build --build-arg UID=$(id -u) -t arrow-debug:latest -f Dockerfile .

: '
# Example run and profile
docker run --privileged -it -v /home/cjoy/src/ArrowPlayground:/tmp   arrow-debug:latest bash
export PERF_DIR=/tmp/ColumnReadingPerf/cmake-build-debug-docker_dbarrow
/usr/bin/perf record --freq=100 --call-graph dwarf -q -o $PERF_DIR/arrow_perf.data $PERF_DIR//arrow_benchmarks
perf report -i $PERF_DIR/arrow_perf.data
'