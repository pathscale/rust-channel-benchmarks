#!/bin/bash
export RUSTFLAGS="-C target-cpu=native"
set -euxo pipefail
IFS=$'\n\t'
SLEEP_SEC=2
cd "$(dirname "$0")"

cargo clean
cargo update

mkdir -p target

cargo build --release --bin mpsc
cargo build --release --bin futures-channel
cargo build --release --bin flume
cargo build --release --bin flume-async
cargo build --release --bin crossbeam-channel
cargo build --release --bin async-channel
cargo build --release --bin kanal
cargo build --release --bin kanal-async
cargo build --release --bin crossfire
cargo build --release --bin crossfire-async
cargo build --release --bin futures-intrusive
cargo build --release --bin tokio-mpsc
cargo build --release --bin pipe-connector

# Function to split output into throughput and latency CSVs
split_output() {
    local input="$1"
    local base_name="$2"
    local throughput_file="target/${base_name}.csv"
    local latency_file="target/${base_name}_latency.csv"
    local name="$3"

    # Write headers
    echo "$name" > "$throughput_file"
    echo "$name" > "$latency_file"

    # Split at "--- latency ---" marker
    awk -v tf="$throughput_file" -v lf="$latency_file" '
    BEGIN { in_latency=0 }
    /^--- latency ---/ { in_latency=1; next }
    in_latency { print >> lf }
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

sleep $SLEEP_SEC
echo "kanal"
./target/release/kanal > /tmp/kanal_output.txt
split_output /tmp/kanal_output.txt kanal "kanal"

sleep $SLEEP_SEC
echo "kanal-async"
./target/release/kanal-async > /tmp/kanal-async_output.txt
split_output /tmp/kanal-async_output.txt kanal-async "kanal-async"

sleep $SLEEP_SEC
echo "crossfire"
./target/release/crossfire > /tmp/crossfire_output.txt
split_output /tmp/crossfire_output.txt crossfire "crossfire"

sleep $SLEEP_SEC
echo "crossfire-async"
./target/release/crossfire-async > /tmp/crossfire-async_output.txt
split_output /tmp/crossfire-async_output.txt crossfire-async "crossfire-async"

#sleep $SLEEP_SEC
#echo "futures-intrusive"
#./target/release/futures-intrusive > /tmp/futures-intrusive_output.txt
#split_output /tmp/futures-intrusive_output.txt futures-intrusive "futures-intrusive"

sleep $SLEEP_SEC
echo "tokio-mpsc"
./target/release/tokio-mpsc > /tmp/tokio-mpsc_output.txt
split_output /tmp/tokio-mpsc_output.txt tokio-mpsc "tokio-mpsc"

sleep $SLEEP_SEC
echo "pipe-connector"
./target/release/pipe-connector > /tmp/pipe-connector_output.txt
split_output /tmp/pipe-connector_output.txt pipe-connector "pipe-connector"

uv run python plot.py target/*.csv
uv run python table.py target/*.csv

echo "Test Environment:"
uname -srvp
rustc --version