use futures::channel::mpsc;
use futures::executor::{block_on, ThreadPool};
use futures::{future, stream, SinkExt, StreamExt};

std::include!("z_types.rs");
std::include!("settings.rs");
std::include!("z_run.rs");

use std::sync::Mutex;

fn spsc_bounded<T: BenchType + 'static>(cap: usize) {
    let pool = ThreadPool::new().unwrap();
    block_on(async {
        let (mut tx, rx) = mpsc::channel(cap);

        pool.spawn_ok(async move {
            tx.send_all(&mut stream::iter((1..MESSAGES + 1).map(T::new).map(Ok)))
                .await
                .unwrap()
        });

        rx.for_each(|t| {
            t.test();
            future::ready(())
        })
        .await
    });
}

fn mpsc_bounded<T: BenchType + 'static>(cap: usize) {
    let pool = ThreadPool::new().unwrap();
    block_on(async {
        let (tx, rx) = mpsc::channel(cap);

        for _ in 0..THREADS {
            let mut tx = tx.clone();
            pool.spawn_ok(async move {
                tx.send_all(&mut stream::iter(
                    (1..MESSAGES / THREADS + 1).map(T::new).map(Ok),
                ))
                .await
                .unwrap()
            });
        }
        drop(tx);

        rx.for_each(|t| {
            t.test();
            future::ready(())
        })
        .await
    });
}

fn spsc_latency<T: BenchType + 'static>(cap: usize) -> LatencyStats {
    let mut stats = LatencyStats::new();
    let pool = ThreadPool::new().unwrap();
    block_on(async {
        let (mut tx, rx) = mpsc::channel(cap);

        pool.spawn_ok(async move {
            for i in 1..LATENCY_MESSAGES + 1 {
                tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });

        rx.for_each(|msg| {
            msg.test();
            stats.add(msg.latency().as_nanos() as u64);
            future::ready(())
        })
        .await
    });
    stats
}

fn mpsc_latency<T: BenchType + 'static>(cap: usize) -> LatencyStats {
    let stats = Mutex::new(LatencyStats::new());
    let pool = ThreadPool::new().unwrap();
    block_on(async {
        let (tx, rx) = mpsc::channel(cap);

        for _ in 0..THREADS {
            let mut tx = tx.clone();
            pool.spawn_ok(async move {
                for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                    tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
                }
            });
        }
        drop(tx);

        rx.for_each(|msg| {
            msg.test();
            stats.lock().unwrap().add(msg.latency().as_nanos() as u64);
            future::ready(())
        })
        .await
    });
    stats.into_inner().unwrap()
}

fn main() {
    println!("futures-channel");

    run!("bounded1_mpsc(empty)", mpsc_bounded::<BenchEmpty>(1));
    run!("bounded1_spsc(empty)", spsc_bounded::<BenchEmpty>(1));

    run!("bounded1_mpsc(usize)", mpsc_bounded::<BenchUsize>(1));
    run!("bounded1_spsc(usize)", spsc_bounded::<BenchUsize>(1));

    run!("bounded1_mpsc(big)", mpsc_bounded::<BenchFixedArray>(1));
    run!("bounded1_spsc(big)", spsc_bounded::<BenchFixedArray>(1));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(1));
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(1));
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(1));
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(1));
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(1));
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(1));
}