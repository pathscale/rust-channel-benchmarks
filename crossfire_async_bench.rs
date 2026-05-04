use crossfire::mpmc::One;
use crossfire::{MAsyncTx, MAsyncRx};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

use std::marker::Unpin;

fn new<T: Send + Unpin + 'static>() -> (MAsyncTx<One<T>>, MAsyncRx<One<T>>) {
    crossfire::mpmc::build(One::new())
}

async fn seq<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();

    for i in 1..MESSAGES + 1 {
        tx.send(T::new(i)).await.unwrap();
    }

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }
}

async fn spsc<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();

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

async fn mpsc<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();
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

async fn mpmc<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();
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

async fn spsc_latency<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();
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

async fn mpsc_latency<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();
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

async fn mpmc_latency<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();
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