use bounded_spsc_queue::make;

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn spsc<T: BenchType + Send>(cap: usize) {
    let (p, c) = make::<T>(cap);

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..MESSAGES + 1 {
                p.push(T::new(i));
            }
        });

        for _ in 0..MESSAGES {
            c.pop().test();
        }
    })
    .unwrap();
}

fn spsc_latency<T: BenchType + Send>(cap: usize) -> LatencyStats {
    let (p, c) = make::<LatencyMessage<T>>(cap);
    let mut stats = LatencyStats::new();

    crossbeam::scope(|scope| {
        scope.spawn(move |_| {
            for i in 1..LATENCY_MESSAGES + 1 {
                p.push(LatencyMessage::new(T::new(i)));
            }
        });

        for _ in 0..LATENCY_MESSAGES {
            let msg = c.pop();
            msg.test();
            stats.add(msg.latency().as_nanos() as u64);
        }
    })
    .unwrap();

    stats
}

fn main() {
    println!("bounded-spsc-queue");

    run!("bounded1_spsc(empty)", spsc::<BenchEmpty>(1));
    run!("bounded1_spsc(usize)", spsc::<BenchUsize>(1));
    run!("bounded1_spsc(big)", spsc::<BenchFixedArray>(1));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(1));
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(1));
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(1));

}
