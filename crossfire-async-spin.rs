use crossfire::mpmc::One;
use crossfire::{MAsyncTx, MAsyncRx, TrySendError, TryRecvError, SendError, RecvError};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

use std::marker::Unpin;

const SPIN_ITERATIONS: usize = 256;

fn new<T: Send + Unpin + 'static>() -> (MAsyncTx<One<T>>, MAsyncRx<One<T>>) {
    crossfire::mpmc::build(One::new())
}

async fn send_spin<T: Send + Unpin + 'static>(
    tx: &MAsyncTx<One<T>>,
    mut val: T,
) -> Result<(), SendError<T>> {
    for _ in 0..SPIN_ITERATIONS {
        match tx.try_send(val) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(v)) => {
                val = v;
                std::hint::spin_loop();
            }
            Err(TrySendError::Disconnected(v)) => return Err(SendError(v)),
        }
    }
    tx.send(val).await
}

async fn recv_spin<T: Send + Unpin + 'static>(
    rx: &MAsyncRx<One<T>>,
) -> Result<T, RecvError> {
    for _ in 0..SPIN_ITERATIONS {
        match rx.try_recv() {
            Ok(item) => return Ok(item),
            Err(TryRecvError::Empty) => {
                std::hint::spin_loop();
            }
            Err(TryRecvError::Disconnected) => return Err(RecvError {}),
        }
    }
    rx.recv().await
}

async fn seq<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();

    for i in 1..MESSAGES + 1 {
        send_spin(&tx, T::new(i)).await.unwrap();
    }

    for _ in 0..MESSAGES {
        recv_spin(&rx).await.unwrap().test();
    }
}

async fn spsc<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();

    let htx = tokio::spawn(async move {
        for i in 1..MESSAGES + 1 {
            send_spin(&tx, T::new(i)).await.unwrap();
        }
    });

    for _ in 0..MESSAGES {
        recv_spin(&rx).await.unwrap().test();
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
                send_spin(&tx, T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..MESSAGES {
        recv_spin(&rx).await.unwrap().test();
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
                send_spin(&tx, T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..THREADS {
        let rx = rx.clone();
        let h = tokio::spawn(async move {
            for _ in 0..MESSAGES / THREADS {
                recv_spin(&rx).await.unwrap().test();
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
            send_spin(&tx, LatencyMessage::new(T::new(i))).await.unwrap();
        }
    });

    for _ in 0..LATENCY_MESSAGES {
        let msg = recv_spin(&rx).await.unwrap();
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
                send_spin(&tx, LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..LATENCY_MESSAGES {
        let msg = recv_spin(&rx).await.unwrap();
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
                send_spin(&tx, LatencyMessage::new(T::new(i))).await.unwrap();
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
                    let msg = recv_spin(&rx).await.unwrap();
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
    println!("crossfire-async-spin");

    run_async!("bounded1_mpmc(empty)", mpmc::<BenchEmpty>());
    run_async!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>());
    run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>());

    run_async!("bounded1_mpmc(usize)", mpmc::<BenchUsize>());
    run_async!("bounded1_mpsc(usize)", mpsc::<BenchUsize>());
    run_async!("bounded1_spsc(usize)", spsc::<BenchUsize>());

    run_async!("bounded1_mpmc(big)", mpmc::<BenchFixedArray>());
    run_async!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>());
    run_async!("bounded1_spsc(big)", spsc::<BenchFixedArray>());

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>().await);
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>().await);
    run_latency!("bounded1_mpmc(empty)", mpmc_latency::<BenchEmpty>().await);
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>().await);
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>().await);
    run_latency!("bounded1_mpmc(usize)", mpmc_latency::<BenchUsize>().await);
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>().await);
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>().await);
    run_latency!("bounded1_mpmc(big)", mpmc_latency::<BenchFixedArray>().await);
}