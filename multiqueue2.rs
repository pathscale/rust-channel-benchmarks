use multiqueue2::mpmc_queue;
use std::sync::mpsc::TrySendError;

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

macro_rules! send_blocking {
    ($tx:expr, $val:expr) => {
        loop {
            match $tx.try_send($val) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                Err(TrySendError::Disconnected(_)) => panic!("disconnected"),
            }
        }
    };
}

fn spsc<T: BenchType + Send + Clone + 'static>() {
    let (tx, rx) = mpmc_queue::<T>(1);

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..MESSAGES + 1 {
                send_blocking!(tx, T::new(i));
            }
        });

        for _ in 0..MESSAGES {
            rx.recv().unwrap().test();
        }
    })
    .unwrap();
}

fn mpsc<T: BenchType + Send + Clone + 'static>() {
    let (tx, rx) = mpmc_queue::<T>(1);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..MESSAGES / THREADS + 1 {
                    send_blocking!(tx, T::new(i));
                }
            });
        }

        for _ in 0..MESSAGES {
            rx.recv().unwrap().test();
        }
    })
    .unwrap();
}

fn mpmc<T: BenchType + Send + Clone + 'static>() {
    let (tx, rx) = mpmc_queue::<T>(1);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..MESSAGES / THREADS + 1 {
                    send_blocking!(tx, T::new(i));
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

fn spsc_latency<T: BenchType + Send + Clone + 'static>() -> LatencyStats {
    let (tx, rx) = mpmc_queue::<LatencyMessage<T>>(1);
    let mut stats = LatencyStats::new();

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..LATENCY_MESSAGES + 1 {
                send_blocking!(tx, LatencyMessage::new(T::new(i)));
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

fn mpsc_latency<T: BenchType + Send + Clone + 'static>() -> LatencyStats {
    let (tx, rx) = mpmc_queue::<LatencyMessage<T>>(1);
    let mut stats = LatencyStats::new();

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                    send_blocking!(tx, LatencyMessage::new(T::new(i)));
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

fn mpmc_latency<T: BenchType + Send + Clone + 'static>() -> LatencyStats {
    let (tx, rx) = mpmc_queue::<LatencyMessage<T>>(1);

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            let tx = tx.clone();
            scope.spawn(move |_| {
                for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                    send_blocking!(tx, LatencyMessage::new(T::new(i)));
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
    println!("multiqueue2");

    run!("bounded1_mpmc(empty)", mpmc::<BenchEmpty>());
    run!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>());
    run!("bounded1_spsc(empty)", spsc::<BenchEmpty>());

    run!("bounded1_mpmc(usize)", mpmc::<BenchUsize>());
    run!("bounded1_mpsc(usize)", mpsc::<BenchUsize>());
    run!("bounded1_spsc(usize)", spsc::<BenchUsize>());

    run!("bounded1_mpmc(big)", mpmc::<BenchFixedArray>());
    run!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>());
    run!("bounded1_spsc(big)", spsc::<BenchFixedArray>());

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>());
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>());
    run_latency!("bounded1_mpmc(empty)", mpmc_latency::<BenchEmpty>());
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>());
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>());
    run_latency!("bounded1_mpmc(usize)", mpmc_latency::<BenchUsize>());
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>());
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>());
    run_latency!("bounded1_mpmc(big)", mpmc_latency::<BenchFixedArray>());
}
