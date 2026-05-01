use tokio::sync::mpsc::{channel, Receiver, Sender};

std::include!("settings.rs");
std::include!("z_types.rs");
std::include!("z_run.rs");

fn new<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    channel(cap)
}

async fn spsc<T: BenchType + 'static>(cap: usize) {
    let (tx, mut rx) = new(cap);

    let htx = tokio::spawn(async move {
        for i in 1..MESSAGES + 1 {
            tx.send(T::new(i)).await.unwrap();
        }
    });

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }

    htx.await.unwrap();
}

async fn mpsc<T: BenchType + 'static>(cap: usize) {
    let (tx, mut rx) = new(cap);
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..MESSAGES / THREADS + 1 {
                tx.send(T::new(i)).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..MESSAGES {
        rx.recv().await.unwrap().test();
    }

    for h in list {
        h.await.unwrap();
    }
}

async fn spsc_latency<T: BenchType + 'static>(cap: usize) -> LatencyStats {
    let (tx, mut rx) = new::<LatencyMessage<T>>(cap);
    let mut stats = LatencyStats::new();

    let htx = tokio::spawn(async move {
        for i in 1..LATENCY_MESSAGES + 1 {
            tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
        }
    });

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.recv().await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    htx.await.unwrap();
    stats
}

async fn mpsc_latency<T: BenchType + 'static>(cap: usize) -> LatencyStats {
    let (tx, mut rx) = new::<LatencyMessage<T>>(cap);
    let mut stats = LatencyStats::new();
    let mut list = Vec::new();

    for _ in 0..THREADS {
        let tx = tx.clone();
        let h = tokio::spawn(async move {
            for i in 1..LATENCY_MESSAGES / THREADS + 1 {
                tx.send(LatencyMessage::new(T::new(i))).await.unwrap();
            }
        });
        list.push(h);
    }

    for _ in 0..LATENCY_MESSAGES {
        let msg = rx.recv().await.unwrap();
        msg.test();
        stats.add(msg.latency().as_nanos() as u64);
    }

    for h in list {
        h.await.unwrap();
    }

    stats
}

#[tokio::main]
async fn main() {
    println!("tokio::sync::mpsc");

    run_async!("bounded1_mpsc(empty)", mpsc::<BenchEmpty>(1));
    run_async!("bounded1_spsc(empty)", spsc::<BenchEmpty>(1));

    run_async!("bounded1_mpsc(usize)", mpsc::<BenchUsize>(1));
    run_async!("bounded1_spsc(usize)", spsc::<BenchUsize>(1));

    run_async!("bounded1_mpsc(big)", mpsc::<BenchFixedArray>(1));
    run_async!("bounded1_spsc(big)", spsc::<BenchFixedArray>(1));

    println!("--- latency ---");
    run_latency!("bounded1_spsc(empty)", spsc_latency::<BenchEmpty>(1).await);
    run_latency!("bounded1_mpsc(empty)", mpsc_latency::<BenchEmpty>(1).await);
    run_latency!("bounded1_spsc(usize)", spsc_latency::<BenchUsize>(1).await);
    run_latency!("bounded1_mpsc(usize)", mpsc_latency::<BenchUsize>(1).await);
    run_latency!("bounded1_spsc(big)", spsc_latency::<BenchFixedArray>(1).await);
    run_latency!("bounded1_mpsc(big)", mpsc_latency::<BenchFixedArray>(1).await);
}