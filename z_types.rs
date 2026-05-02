use std::time::{Duration, Instant};

trait BenchType: Send + Sync {
    fn new(v: usize) -> Self;
    // ensure that received data is tested and used to avoid compiler optimizer removing receive part.
    fn test(&self);
}


#[derive(Debug, Clone)]
struct LatencyMessage<T> {
    send_time: Instant,
    payload: T,
}

impl<T: BenchType> LatencyMessage<T> {
    fn new(payload: T) -> Self {
        Self {
            send_time: Instant::now(),
            payload,
        }
    }

    fn latency(&self) -> Duration {
        Instant::now().duration_since(self.send_time)
    }

    fn test(&self) {
        self.payload.test();
    }
}

#[derive(Debug)]
struct LatencyStats {
    samples: Vec<u64>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(LATENCY_MESSAGES),
        }
    }

    fn add(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    fn merge(&mut self, other: LatencyStats) {
        self.samples.extend(other.samples);
    }

    fn compute(&mut self) -> (u64, u64, u64, u64, u64, u64) {
        self.samples.sort_unstable();
        let n = self.samples.len();
        if n == 0 {
            return (0, 0, 0, 0, 0, 0);
        }
        let min = self.samples[0];
        let max = self.samples[n - 1];
        let p50 = self.samples[n / 2];
        let p90 = self.samples[(n * 90) / 100];
        let p99 = self.samples[(n * 99) / 100];
        let p999 = self.samples[(n * 999) / 1000];
        (min, p50, p90, p99, p999, max)
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchEmpty {}

#[derive(Debug, Clone, Copy)]
struct BenchUsize {
    a: usize,
}

#[derive(Debug, Clone, Copy)]
struct BenchFixedArray([usize; 4]);

impl BenchType for BenchEmpty {
    fn new(_v: usize) -> Self {
        Self {}
    }
    #[inline(always)]
    fn test(&self) {}
}

impl BenchType for BenchUsize {
    fn new(v: usize) -> Self {
        return Self { a: v };
    }
    #[inline(always)]
    fn test(&self) {
        if self.a < 1 {
            panic!("invalid_value")
        }
    }
}

impl BenchType for BenchFixedArray {
    fn new(v: usize) -> Self {
        return BenchFixedArray([v; 4]);
    }

    #[inline(always)]
    fn test(&self) {
        // only to ensure result is being used and compiler does not remove bench type
        if self.0[0] < 1 {
            panic!("invalid_value")
        }
    }
}
