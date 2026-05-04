use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

std::include!("crossfire_bench.rs");

fn main() {
    println!("crossfire-mimalloc");

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