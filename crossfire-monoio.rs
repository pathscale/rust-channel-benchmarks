use crossfire::mpmc::One;
use crossfire::{MAsyncTx, MAsyncRx};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

use std::marker::Unpin;
use std::thread;

fn new<T: Send + Unpin + 'static>() -> (MAsyncTx<One<T>>, MAsyncRx<One<T>>) {
    crossfire::mpmc::build(One::new())
}

fn spsc<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();

    let producer = thread::spawn(move || {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .expect("Failed to build runtime")
            .block_on(async move {
                for i in 1..MESSAGES + 1 {
                    tx.send(T::new(i)).await.unwrap();
                }
            });
    });

    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .expect("Failed to build runtime")
        .block_on(async {
            for _ in 0..MESSAGES {
                rx.recv().await.unwrap().test();
            }
        });

    producer.join().unwrap();
}

fn mpsc_threaded<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();
    let mut producers = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .expect("Failed to build runtime")
                .block_on(async move {
                    for i in 1..MESSAGES / THREADS + 1 {
                        tx.send(T::new(i)).await.unwrap();
                    }
                });
        });
        producers.push(h);
    }

    monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .expect("Failed to build runtime")
        .block_on(async {
            for _ in 0..MESSAGES {
                rx.recv().await.unwrap().test();
            }
        });

    for h in producers {
        h.join().unwrap();
    }
}

fn mpmc_threaded<T: BenchType + Send + Unpin + 'static>() {
    let (tx, rx) = new();
    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .expect("Failed to build runtime")
                .block_on(async move {
                    for i in 1..MESSAGES / THREADS + 1 {
                        tx.send(T::new(i)).await.unwrap();
                    }
                });
        });
        handles.push(h);
    }

    for _ in 0..THREADS {
        let rx = rx.clone();
        let h = thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .expect("Failed to build runtime")
                .block_on(async move {
                    for _ in 0..MESSAGES / THREADS {
                        rx.recv().await.unwrap().test();
                    }
                });
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
}

fn spsc_latency<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();

    let producer = thread::spawn(move || {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .expect("Failed to build runtime")
            .block_on(async move {
                for i in 1..LATENCY_MESSAGES + 1 {
                    tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
                }
            });
    });

    let stats = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .expect("Failed to build runtime")
        .block_on(async {
            let mut stats = LatencyStats::new();
            for _ in 0..LATENCY_MESSAGES {
                let msg = rx.recv().await.unwrap();
                msg.test();
                stats.add(msg.latency().as_nanos() as u64);
            }
            stats
        });

    producer.join().unwrap();
    stats
}

fn mpsc_latency_threaded<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();
    let mut producers = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .expect("Failed to build runtime")
                .block_on(async move {
                    for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                        tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
                    }
                });
        });
        producers.push(h);
    }

    let stats = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .build()
        .expect("Failed to build runtime")
        .block_on(async {
            let mut stats = LatencyStats::new();
            for _ in 0..LATENCY_MESSAGES {
                let msg = rx.recv().await.unwrap();
                msg.test();
                stats.add(msg.latency().as_nanos() as u64);
            }
            stats
        });

    for h in producers {
        h.join().unwrap();
    }

    stats
}

fn mpmc_latency_threaded<T: BenchType + Send + Unpin + 'static>() -> LatencyStats {
    let (tx, rx) = new();
    let mut handles = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = thread::spawn(move || {
            monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .expect("Failed to build runtime")
                .block_on(async move {
                    for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                        tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
                    }
                });
        });
        handles.push(h);
    }

    let consumer_handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || {
                monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .build()
                    .expect("Failed to build runtime")
                    .block_on(async move {
                        let mut stats = LatencyStats::new();
                        for _ in 0..LATENCY_MESSAGES / THREADS {
                            let msg = rx.recv().await.unwrap();
                            msg.test();
                            stats.add(msg.latency().as_nanos() as u64);
                        }
                        stats
                    })
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let mut merged = LatencyStats::new();
    for h in consumer_handles {
        merged.merge(h.join().unwrap());
    }
    merged
}

fn main() {
    println!("crossfire-monoio");

    run!("bounded1_spsc(empty)", spsc::<BenchEmpty>());
    run!("bounded1_mpsc(empty)", mpsc_threaded::<BenchEmpty>());
    run!("bounded1_mpmc(empty)", mpmc_threaded::<BenchEmpty>());

    run!("bounded1_spsc(usize)", spsc::<BenchUsize>());
    run!("bounded1_mpsc(usize)", mpsc_threaded::<BenchUsize>());
    run!("bounded1_mpmc(usize)", mpmc_threaded::<BenchUsize>());

    run!("bounded1_spsc(big)", spsc::<BenchFixedArray>());
    run!("bounded1_mpsc(big)", mpsc_threaded::<BenchFixedArray>());
    run!("bounded1_mpmc(big)", mpmc_threaded::<BenchFixedArray>());

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>());
    run_latency!("bounded1_mpsc(empty)", mpsc_latency_threaded::<BenchEmpty>());
    run_latency!("bounded1_mpmc(empty)", mpmc_latency_threaded::<BenchEmpty>());
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>());
    run_latency!("bounded1_mpsc(usize)", mpsc_latency_threaded::<BenchUsize>());
    run_latency!("bounded1_mpmc(usize)", mpmc_latency_threaded::<BenchUsize>());
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>());
    run_latency!("bounded1_mpsc(big)", mpsc_latency_threaded::<BenchFixedArray>());
    run_latency!("bounded1_mpmc(big)", mpmc_latency_threaded::<BenchFixedArray>());
}