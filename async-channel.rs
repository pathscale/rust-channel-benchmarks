use async_channel::{bounded, unbounded, Receiver, Sender};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn new<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
    match cap {
        None => unbounded(),
        Some(cap) => bounded(cap),
    }
}

async fn seq<T: BenchType + 'static>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    for i in 1..MESSAGES + 1 {
        tx.send(T::new(i)).await.unwrap();
    }

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }
}

async fn spsc<T: BenchType + 'static>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    let htx = tokio::spawn(async move {
        for i in 1..MESSAGES + 1 {
            tx.send(T::new(i)).await.unwrap();
        }
    });

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }

    htx.await.unwrap();
}

async fn mpsc<T: BenchType + 'static>(cap: Option<usize>) {
    let (tx, rx) = new(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..MESSAGES / THREADS + 1 {
                tx.send(T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }

    for h in list {
        h.await.unwrap();
    }
}

async fn mpmc<T: BenchType + 'static>(cap: Option<usize>) {
    let (tx, rx) = new(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..MESSAGES / THREADS + 1 {
                tx.send(T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..THREADS {
        let rx = rx.clone();
        let h = tokio::spawn(async move {
            for _ in 0..MESSAGES / THREADS {
                rx.recv().await.unwrap().test();
            }
        });
        list.push(h);
    }

    for h in list {
        h.await.unwrap();
    }
}

async fn spsc_latency<T: BenchType + 'static>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);
    let mut stats = LatencyStats::new();

    let htx = tokio::spawn(async move {
        for i in 1..LATENCY_MESSAGES + 1 {
            tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
        }
    });

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.recv().await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    htx.await.unwrap();
    stats
}

async fn mpsc_latency<T: BenchType + 'static>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);
    let mut stats = LatencyStats::new();
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.recv().await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    for h in list {
        h.await.unwrap();
    }

    stats
}

async fn mpmc_latency<T: BenchType + 'static>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    let consumer_handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let rx = rx.clone();
            tokio::spawn(async move {
                let mut stats = LatencyStats::new();
                for _ in 0..LATENCY_MESSAGES / THREADS {
                    let msg = rx.recv().await.unwrap();
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
    println!("async-channel");

    run_async!("bounded1_mpmc(empty)", mpmc::<BenchEmpty>(Some(1)));
    run_async!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>(Some(1)));
    run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>(Some(1)));

    run_async!("bounded1_mpmc(usize)", mpmc::<BenchUsize>(Some(1)));
    run_async!("bounded1_mpsc(usize)", mpsc::<BenchUsize>(Some(1)));
    run_async!("bounded1_spsc(usize)", spsc::<BenchUsize>(Some(1)));

    run_async!("bounded1_mpmc(big)", mpmc::<BenchFixedArray>(Some(1)));
    run_async!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>(Some(1)));
    run_async!("bounded1_spsc(big)", spsc::<BenchFixedArray>(Some(1)));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(Some(1)).await);
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(Some(1)).await);
    run_latency!("bounded1_mpmc(empty)", mpmc_latency::<BenchEmpty>(Some(1)).await);
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(Some(1)).await);
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(Some(1)).await);
    run_latency!("bounded1_mpmc(usize)", mpmc_latency::<BenchUsize>(Some(1)).await);
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(Some(1)).await);
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(Some(1)).await);
    run_latency!("bounded1_mpmc(big)", mpmc_latency::<BenchFixedArray>(Some(1)).await);
}