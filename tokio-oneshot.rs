use tokio::sync::oneshot;

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

async fn seq<T: BenchType + std::fmt::Debug>() {
    for i in 0..MESSAGES {
        let (tx, rx) = oneshot::channel::<T>();
        tx.send(T::new(i + 1)).unwrap();
        rx.await.unwrap().test();
    }
}

async fn spsc<T: BenchType + std::fmt::Debug + 'static>() {
    let (meta_tx, mut meta_rx) = tokio::sync::mpsc::channel::<oneshot::Receiver<T>>(1);

    let producer = tokio::spawn(async move {
        for i in 1..MESSAGES + 1 {
            let (tx, rx) = oneshot::channel::<T>();
            meta_tx.send(rx).await.unwrap();
            tx.send(T::new(i)).unwrap();
        }
    });

    while let Some(rx) = meta_rx.recv().await {
        rx.await.unwrap().test();
    }

    producer.await.unwrap();
}

async fn spsc_latency<T: BenchType + std::fmt::Debug + 'static>() -> LatencyStats {
    let (meta_tx, mut meta_rx) = tokio::sync::mpsc::channel::<oneshot::Receiver<LatencyMessage<T>>>(1);
    let mut stats = LatencyStats::new();

    let producer = tokio::spawn(async move {
        for i in 1..LATENCY_MESSAGES + 1 {
            let (tx, rx) = oneshot::channel::<LatencyMessage<T>>();
            meta_tx.send(rx).await.unwrap();
            tx.send(LatencyMessage::new(T::new(i))).unwrap();
        }
    });

    while let Some(rx) = meta_rx.recv().await {
        let msg = rx.await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    producer.await.unwrap();
    stats
}

#[tokio::main]
async fn main() {
    println!("tokio-oneshot");

    run_async!("bounded1_seq(empty)", seq::<BenchEmpty>());
    run_async!("bounded1_seq(usize)", seq::<BenchUsize>());
    run_async!("bounded1_seq(big)", seq::<BenchFixedArray>());

    run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>());
    run_async!("bounded1_spsc(usize)", spsc::<BenchUsize>());
    run_async!("bounded1_spsc(big)", spsc::<BenchFixedArray>());

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>().await);
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>().await);
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>().await);
}
