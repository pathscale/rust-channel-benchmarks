#!/bin/bash
export RUSTFLAGS="-C target-cpu=native"
set -euo pipefail
IFS=$'\n\t'
SLEEP_SEC=2
cd "$(dirname "$0")"

cargo clean
cargo update

mkdir -p target

#cargo build --release --bin mpsc
#cargo build --release --bin futures-channel
#cargo build --release --bin flume
#cargo build --release --bin flume-async
#cargo build --release --bin crossbeam-channel
#cargo build --release --bin async-channel
#cargo build --release --bin crossfire
cargo build --release --bin crossfire-async
#cargo build --release --bin crossfire-jemalloc
cargo build --release --bin crossfire-async-jemalloc
#cargo build --release --bin crossfire-mimalloc
cargo build --release --bin crossfire-async-mimalloc
#cargo build --release --bin crossfire-monoio
cargo build --release --bin crossfire-smol
cargo build --release --bin crossfire-async-spin
#cargo build --release --bin futures-intrusive
#cargo build --release --bin pipe-connector
#cargo build --release --bin tokio-oneshot
cargo build --release --bin bounded-spsc-queue
#cargo build --release --bin multiqueue2

# Function to split output into throughput and latency CSVs
split_output() {
    local input="$1"
    local base_name="$2"
    local throughput_file="target/${base_name}.csv"
    local latency_file="target/${base_name}_latency.csv"
    local name="$3"

    # Extract the benchmark name (first line) for the latency file header
    local name=$(head -1 "$input")

    # Split at "--- latency ---" marker
    awk -v tf="$throughput_file" -v lf="$latency_file" -v name="$name" '
    BEGIN { in_latency=0; latency_header_written=0 }
    /^--- latency ---/ { in_latency=1; next }
    in_latency {
        if (!latency_header_written) {
            print name >> lf
            latency_header_written=1
        }
        print >> lf
    }
    !in_latency { print >> tf }
    ' "$input"
}

#sleep $SLEEP_SEC
#echo "mpsc"
#./target/release/mpsc > /tmp/mpsc_output.txt
#split_output /tmp/mpsc_output.txt mpsc "std::mpsc"

#sleep $SLEEP_SEC
#echo "futures-channel"
#./target/release/futures-channel > /tmp/futures-channel_output.txt
#split_output /tmp/futures-channel_output.txt futures-channel "futures-channel"
#
#sleep $SLEEP_SEC
#echo "flume"
#./target/release/flume > /tmp/flume_output.txt
#split_output /tmp/flume_output.txt flume "flume"

#sleep $SLEEP_SEC
#echo "flume-async"
#./target/release/flume-async > /tmp/flume-async_output.txt
#split_output /tmp/flume-async_output.txt flume-async "flume-async"

#sleep $SLEEP_SEC
#echo "crossbeam-channel"
#./target/release/crossbeam-channel > /tmp/crossbeam-channel_output.txt
#split_output /tmp/crossbeam-channel_output.txt crossbeam-channel "crossbeam-channel"
#
#sleep $SLEEP_SEC
#echo "async-channel"
#./target/release/async-channel > /tmp/async-channel_output.txt
#split_output /tmp/async-channel_output.txt async-channel "async-channel"

#sleep $SLEEP_SEC
#echo "crossfire"
#./target/release/crossfire > /tmp/crossfire_output.txt
#split_output /tmp/crossfire_output.txt crossfire "crossfire"

sleep $SLEEP_SEC
echo "crossfire-async"
./target/release/crossfire-async > /tmp/crossfire-async_output.txt
split_output /tmp/crossfire-async_output.txt crossfire-async "crossfire-async"

#sleep $SLEEP_SEC
#echo "crossfire-monoio"
#./target/release/crossfire-monoio > /tmp/crossfire-monoio_output.txt
#split_output /tmp/crossfire-monoio_output.txt crossfire-monoio "crossfire-monoio"

sleep $SLEEP_SEC
echo "crossfire-smol"
./target/release/crossfire-smol > /tmp/crossfire-smol_output.txt
split_output /tmp/crossfire-smol_output.txt crossfire-smol "crossfire-smol"

sleep $SLEEP_SEC
echo "crossfire-async-spin"
./target/release/crossfire-async-spin > /tmp/crossfire-async-spin_output.txt
split_output /tmp/crossfire-async-spin_output.txt crossfire-async-spin "crossfire-async-spin"

#sleep $SLEEP_SEC
#echo "crossfire-jemalloc"
#./target/release/crossfire-jemalloc > /tmp/crossfire-jemalloc_output.txt
#split_output /tmp/crossfire-jemalloc_output.txt crossfire-jemalloc "crossfire-jemalloc"
#
sleep $SLEEP_SEC
echo "crossfire-async-jemalloc"
./target/release/crossfire-async-jemalloc > /tmp/crossfire-async-jemalloc_output.txt
split_output /tmp/crossfire-async-jemalloc_output.txt crossfire-async-jemalloc "crossfire-async-jemalloc"
#
#sleep $SLEEP_SEC
#echo "crossfire-mimalloc"
#./target/release/crossfire-mimalloc > /tmp/crossfire-mimalloc_output.txt
#split_output /tmp/crossfire-mimalloc_output.txt crossfire-mimalloc "crossfire-mimalloc"
#
sleep $SLEEP_SEC
echo "crossfire-async-mimalloc"
./target/release/crossfire-async-mimalloc > /tmp/crossfire-async-mimalloc_output.txt
split_output /tmp/crossfire-async-mimalloc_output.txt crossfire-async-mimalloc "crossfire-async-mimalloc"

#sleep $SLEEP_SEC
#echo "futures-intrusive"
#./target/release/futures-intrusive > /tmp/futures-intrusive_output.txt
#split_output /tmp/futures-intrusive_output.txt futures-intrusive "futures-intrusive"

# sleep $SLEEP_SEC
# echo "pipe-connector"
# ./target/release/pipe-connector > /tmp/pipe-connector_output.txt
# split_output /tmp/pipe-connector_output.txt pipe-connector "pipe-connector"

#sleep $SLEEP_SEC
#echo "tokio-oneshot"
#./target/release/tokio-oneshot > /tmp/tokio-oneshot_output.txt
#split_output /tmp/tokio-oneshot_output.txt tokio-oneshot "tokio-oneshot"

sleep $SLEEP_SEC
echo "bounded-spsc-queue"
./target/release/bounded-spsc-queue > /tmp/bounded-spsc-queue_output.txt
split_output /tmp/bounded-spsc-queue_output.txt bounded-spsc-queue "bounded-spsc-queue"

#sleep $SLEEP_SEC
#echo "multiqueue2"
#./target/release/multiqueue2 > /tmp/multiqueue2_output.txt
#split_output /tmp/multiqueue2_output.txt multiqueue2 "multiqueue2"

uv run python plot.py target/*.csv
uv run python table.py target/*.csv

echo "Test Environment:"
uname -srvp
rustc --version