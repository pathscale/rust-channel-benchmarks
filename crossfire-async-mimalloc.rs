use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

std::include!("crossfire_async_bench.rs");

#[tokio::main]
async fn main() {
    println!("crossfire-async-mimalloc");

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