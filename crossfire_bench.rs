use crossfire::mpmc::One;
use crossfire::{MTx, MRx};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn new<T: Send + 'static>() -> (MTx<One<T>>, MRx<One<T>>) {
    crossfire::mpmc::build(One::new())
}

fn seq<T: BenchType + Send + 'static>() {
    let (tx, rx) = new();

    for i in 1..MESSAGES + 1 {
        tx.send(T::new(i)).unwrap();
    }

    for _ in 0..MESSAGES {
        rx.recv().unwrap().test();
    }
}

fn spsc<T: BenchType + Send + 'static>() {
    let (tx, rx) = new();

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

fn mpsc<T: BenchType + Send + 'static>() {
    let (tx, rx) = new();

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

fn mpmc<T: BenchType + Send + 'static>() {
    let (tx, rx) = new();

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

fn spsc_latency<T: BenchType + Send + 'static>() -> LatencyStats {
    let (tx, rx) = new();
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
            let lat = msg.latency().as_nanos();
            println!("{}ns", lat);
            stats.add(lat as u64);
        }
    })
    .unwrap();

    stats
}

fn mpsc_latency<T: BenchType + Send + 'static>() -> LatencyStats {
    let (tx, rx) = new();
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

fn mpmc_latency<T: BenchType + Send + 'static>() -> LatencyStats {
    let (tx, rx) = new();

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