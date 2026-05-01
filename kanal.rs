use kanal::{bounded, unbounded, Receiver, Sender};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn new<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
    match cap {
        None => unbounded(),
        Some(cap) => bounded(cap),
    }
}

fn seq<T: BenchType>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    for i in 1..MESSAGES + 1 {
        tx.send(T::new(i)).unwrap();
    }

    for _ in 0..MESSAGES {
        rx.recv().unwrap().test();
    }
}

fn spsc<T: BenchType>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..MESSAGES + 1 {
                tx.send(T::new(i)).unwrap();
            }
        });

        for _ in 0..MESSAGES {
            rx.recv().unwrap().test();
        }
    })
    .unwrap();
}

fn mpsc<T: BenchType>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..MESSAGES / THREADS + 1 {
                    tx.send(T::new(i)).unwrap();
                }
            });
        }

        for _ in 0..MESSAGES {
            rx.recv().unwrap().test();
        }
    })
    .unwrap();
}

fn mpmc<T: BenchType>(cap: Option<usize>) {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..MESSAGES / THREADS + 1 {
                    tx.send(T::new(i)).unwrap();
                }
            });
        }

        for _ in 0..THREADS {
            let rx = rx.clone();
            scope.spawn(move |_| {
                for _ in 0..MESSAGES / THREADS {
                    rx.recv().unwrap().test();
                }
            });
        }
    })
    .unwrap();
}

fn spsc_latency<T: BenchType>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);
    let mut stats = LatencyStats::new();

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..LATENCY_MESSAGES + 1 {
                tx.send(LatencyMessage::new(T::new(i))).unwrap();
            }
        });

        for _ in 0..LATENCY_MESSAGES {
            let msg = rx.recv().unwrap();
            msg.test();
            stats.add(msg.latency().as_nanos() as u64);
        }
    })
    .unwrap();

    stats
}

fn mpsc_latency<T: BenchType>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);
    let mut stats = LatencyStats::new();

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                    tx.send(LatencyMessage::new(T::new(i))).unwrap();
                }
            });
        }

        for _ in 0..LATENCY_MESSAGES {
            let msg = rx.recv().unwrap();
            msg.test();
            stats.add(msg.latency().as_nanos() as u64);
        }
    })
    .unwrap();

    stats
}

fn mpmc_latency<T: BenchType>(cap: Option<usize>) -> LatencyStats {
    let (tx, rx) = new(cap);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                    tx.send(LatencyMessage::new(T::new(i))).unwrap();
                }
            });
        }

        let consumer_handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let rx = rx.clone();
                scope.spawn(move |_| {
                    let mut stats = LatencyStats::new();
                    for _ in 0..LATENCY_MESSAGES / THREADS {
                        let msg = rx.recv().unwrap();
                        msg.test();
                        stats.add(msg.latency().as_nanos() as u64);
                    }
                    stats
                })
            })
            .collect();

        let mut merged = LatencyStats::new();
        for handle in consumer_handles {
            merged.merge(handle.join().unwrap());
        }
        merged
    })
    .unwrap()
}

fn main() {
    println!("kanal");

    run!("bounded1_mpmc(empty)", mpmc::<BenchEmpty>(Some(1)));
    run!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>(Some(1)));
    run!("bounded1_spsc(empty)", spsc::<BenchEmpty>(Some(1)));

    run!("bounded1_mpmc(usize)", mpmc::<BenchUsize>(Some(1)));
    run!("bounded1_mpsc(usize)", mpsc::<BenchUsize>(Some(1)));
    run!("bounded1_spsc(usize)", spsc::<BenchUsize>(Some(1)));

    run!("bounded1_mpmc(big)", mpmc::<BenchFixedArray>(Some(1)));
    run!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>(Some(1)));
    run!("bounded1_spsc(big)", spsc::<BenchFixedArray>(Some(1)));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(Some(1)));
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(Some(1)));
    run_latency!("bounded1_mpmc(empty)", mpmc_latency::<BenchEmpty>(Some(1)));
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(Some(1)));
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(Some(1)));
    run_latency!("bounded1_mpmc(usize)", mpmc_latency::<BenchUsize>(Some(1)));
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(Some(1)));
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(Some(1)));
    run_latency!("bounded1_mpmc(big)", mpmc_latency::<BenchFixedArray>(Some(1)));
}