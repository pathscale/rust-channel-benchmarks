use std::sync::mpsc;

std::include!("z_types.rs");
std::include!("settings.rs");
std::include!("z_run.rs");

use std::sync::Mutex;

fn spsc_async<T: BenchType>() {
    let (tx, rx) = mpsc::channel();

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

fn spsc_sync<T: BenchType>(cap: usize) {
    let (tx, rx) = mpsc::sync_channel(cap);

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

fn mpsc_async<T: BenchType>() {
    let (tx, rx) = mpsc::channel();

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

fn mpsc_sync<T: BenchType>(cap: usize) {
    let (tx, rx) = mpsc::sync_channel(cap);

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

fn spsc_latency<T: BenchType>(cap: usize) -> LatencyStats {
    let (tx, rx) = mpsc::sync_channel(cap);
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

fn mpsc_latency<T: BenchType>(cap: usize) -> LatencyStats {
    let (tx, rx) = mpsc::sync_channel(cap);
    let stats = Mutex::new(LatencyStats::new());

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
            stats.lock().unwrap().add(msg.latency().as_nanos() as u64);
        }
    })
    .unwrap();

    stats.into_inner().unwrap()
}

fn main() {
    println!("std::mpsc");

    run!("bounded1_mpsc(empty)", mpsc_sync::<BenchEmpty>(1));
    run!("bounded1_spsc(empty)", spsc_sync::<BenchEmpty>(1));

    run!("bounded1_mpsc(usize)", mpsc_sync::<BenchUsize>(1));
    run!("bounded1_spsc(usize)", spsc_sync::<BenchUsize>(1));

    run!("bounded1_mpsc(big)", mpsc_sync::<BenchFixedArray>(1));
    run!("bounded1_spsc(big)", spsc_sync::<BenchFixedArray>(1));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(1));
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(1));
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(1));
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(1));
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(1));
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(1));
}
