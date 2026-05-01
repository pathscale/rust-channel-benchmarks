use futures::{SinkExt, StreamExt};
use std::fmt::Debug;
use benchmarks::pipe_connector::RoundRobinPipeConnector;

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn new<T: Send + 'static>(cap: usize) -> (RoundRobinPipeConnector<T>, RoundRobinPipeConnector<T>) {
    let connector = RoundRobinPipeConnector::new(cap);
    (connector.clone(), connector)
}

async fn spsc<T: BenchType + Debug + Send + Unpin + 'static>(cap: usize) {
    let (sink_conn, stream_conn) = new(cap);
    let mut sink = sink_conn.sink();
    let stream = stream_conn.stream();

    let htx = tokio::spawn(async move {
        for i in 1..MESSAGES + 1 {
            sink.send(T::new(i)).await.unwrap();
        }
    });

    let mut rx = stream;
    for _ in 0..MESSAGES {
        rx.next().await.unwrap().test();
    }

    htx.await.unwrap();
}

async fn mpsc<T: BenchType + Debug + Send + Unpin + 'static>(cap: usize) {
    let (sink_conn, stream_conn) = new(cap);
    let mut rx = stream_conn.stream();
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let mut sink = sink_conn.sink();
        let h = tokio::spawn(async move {
            for i in 1..MESSAGES / THREADS + 1 {
                sink.send(T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..MESSAGES {
        rx.next().await.unwrap().test();
    }

    for h in list {
        h.await.unwrap();
    }
}

async fn mpmc<T: BenchType + Debug + Send + Unpin + 'static>(cap: usize) {
    let (sink_conn, stream_conn) = new(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let mut sink = sink_conn.sink();
        let h = tokio::spawn(async move {
            for i in 1..MESSAGES / THREADS + 1 {
                sink.send(T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..THREADS {
        let rx = stream_conn.stream();
        let h = tokio::spawn(async move {
            let mut rx = rx;
            for _ in 0..MESSAGES / THREADS {
                rx.next().await.unwrap().test();
            }
        });
        list.push(h);
    }

    for h in list {
        h.await.unwrap();
    }
}

async fn spsc_latency<T: BenchType + Debug + Send + Unpin + 'static>(cap: usize) -> LatencyStats {
    let (sink_conn, stream_conn) = new::<LatencyMessage<T>>(cap);
    let mut sink = sink_conn.sink();
    let mut rx = stream_conn.stream();
    let mut stats = LatencyStats::new();

    let htx = tokio::spawn(async move {
        for i in 1..LATENCY_MESSAGES + 1 {
            sink.send(LatencyMessage::new(T::new(i))).await.unwrap();
        }
    });

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.next().await.unwrap();
        msg.test();
        let lat = msg.latency().as_nanos();
        println!("{}ns", lat);
        stats.add(lat as u64);
    }

    htx.await.unwrap();
    stats
}

async fn mpsc_latency<T: BenchType + Debug + Send + Unpin + 'static>(cap: usize) -> LatencyStats {
    let (sink_conn, stream_conn) = new::<LatencyMessage<T>>(cap);
    let mut rx = stream_conn.stream();
    let mut stats = LatencyStats::new();
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let mut sink = sink_conn.sink();
        let h = tokio::spawn(async move {
            for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                sink.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.next().await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    for h in list {
        h.await.unwrap();
    }

    stats
}

async fn mpmc_latency<T: BenchType + Debug + Send + Unpin + Sync + 'static>(cap: usize) -> LatencyStats {
    let (sink_conn, stream_conn) = new::<LatencyMessage<T>>(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let mut sink = sink_conn.sink();
        let h = tokio::spawn(async move {
            for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                sink.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    let consumer_handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let rx = stream_conn.stream();
            tokio::spawn(async move {
                let mut rx = rx;
                let mut stats = LatencyStats::new();
                for _ in 0..LATENCY_MESSAGES / THREADS {
                    let msg = rx.next().await.unwrap();
                    msg.test();
                    stats.add(msg.latency().as_nanos() as u64);
                }
                stats
            })
        })
        .collect();

    for h in list {
        h.await.unwrap();
    }

    let mut merged = LatencyStats::new();
    for handle in consumer_handles {
        merged.merge(handle.await.unwrap());
    }
    merged
}

#[tokio::main]
async fn main() {
    println!("pipe-connector");

    // run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>(3));
    //
    // run_async!("bounded1_mpmc(empty)", mpmc::<BenchEmpty>(1));
    // run_async!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>(1));
    // run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>(1));
    //
    // run_async!("bounded1_mpmc(usize)", mpmc::<BenchUsize>(1));
    // run_async!("bounded1_mpsc(usize)", mpsc::<BenchUsize>(1));
    // run_async!("bounded1_spsc(usize)", spsc::<BenchUsize>(1));
    //
    // run_async!("bounded1_mpmc(big)", mpmc::<BenchFixedArray>(1));
    // run_async!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>(1));
    // run_async!("bounded1_spsc(big)", spsc::<BenchFixedArray>(1));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(1).await);
    // run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(1).await);
    // run_latency!("bounded1_mpmc(empty)", mpmc_latency::<BenchEmpty>(1).await);
    // run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(1).await);
    // run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(1).await);
    // run_latency!("bounded1_mpmc(usize)", mpmc_latency::<BenchUsize>(1).await);
    // run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(1).await);
    // run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(1).await);
    // run_latency!("bounded1_mpmc(big)", mpmc_latency::<BenchFixedArray>(1).await);
}