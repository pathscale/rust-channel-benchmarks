use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use eyre::bail;
use futures::StreamExt;
use futures::{Sink, Stream};
use parking_lot::{FairMutex, RwLock};

pub type BoxedProducer<T> = Box<dyn futures::Stream<Item = eyre::Result<T>> + Send + Unpin>;

#[derive(Debug)]
struct SharedRoundRobinBuffer<T> {
    state: Arc<RoundRobinBufferState<T>>,
}

impl<T> Clone for SharedRoundRobinBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

#[derive(Debug)]
struct RoundRobinBufferState<T> {
    receiver_queues: RwLock<HashMap<u64, RwLock<ReceiverInner<T>>>>,
    send_wakers: FairMutex<Vec<Waker>>,
    receiver_id_gen: AtomicU64,
    capacity: usize,
}

#[derive(Debug)]
struct ReceiverInner<T> {
    queue: VecDeque<T>,
    recv_waker: Option<Waker>,
}

impl<T> SharedRoundRobinBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            state: Arc::new(RoundRobinBufferState {
                receiver_queues: RwLock::new(HashMap::new()),
                capacity,
                receiver_id_gen: AtomicU64::new(0),
                send_wakers: FairMutex::new(vec![]),
            }),
        }
    }

    fn wake_one_sender(&self) {
        let mut g = self.state.send_wakers.lock();
        if let Some(w) = g.pop() {
            w.wake();
        }
    }
}

#[derive(Debug)]
pub struct RoundRobinPipeConnectorSink<T> {
    buffer: SharedRoundRobinBuffer<T>,
}

impl<T> Clone for RoundRobinPipeConnectorSink<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
        }
    }
}

impl<T> Sink<T> for RoundRobinPipeConnectorSink<T>
where
    T: Debug + Send + Unpin + 'static,
{
    type Error = eyre::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let g = self.buffer.state.receiver_queues.read();
        let mut length = 0;
        for (_, l) in g.iter() {
            let g1 = l.read();
            length += g1.queue.len();
        }

        if length < self.buffer.state.capacity {
            Poll::Ready(Ok(()))
        } else {
            // Buffer is full, register waker
            let mut w_g = self.buffer.state.send_wakers.lock();
            w_g.push(cx.waker().clone());
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let g = self.buffer.state.receiver_queues.read();
        if g.is_empty() {
            return Ok(());
        }
        loop {
            for (_, recv_inner) in g.iter() {
                if let Some(mut i) = recv_inner.try_write() {
                    i.queue.push_back(item);
                    if let Some(w) = i.recv_waker.take() {
                        w.wake()
                    }
                    return Ok(());
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub struct RoundRobinPipeConnectorStream<T> {
    id: u64,
    buffer: SharedRoundRobinBuffer<T>,
}

impl<T> Drop for RoundRobinPipeConnectorStream<T> {
    fn drop(&mut self) {
        // Remove this receiver's queue when dropped
        let mut g = self.buffer.state.receiver_queues.write();
        g.remove(&self.id);

        // Wake all senders in case they were waiting for capacity
        drop(g);
        self.buffer.wake_one_sender();
    }
}

impl<T> RoundRobinPipeConnectorStream<T>
where
    T: Debug + Send + 'static,
{
    fn try_steal_item(&self) -> Option<T> {
        let g = self.buffer.state.receiver_queues.read();
        for (id, l) in g.iter() {
            if *id == self.id {
                continue;
            }

            let mut m_g = l.write();
            if let Some(item) = m_g.queue.pop_front() {
                return Some(item);
            }
        }

        None
    }

    pub fn into_producer(self) -> BoxedProducer<T>
    where
        T: Sync,
    {
        Box::new(self.map(Ok))
    }
}

impl<T> Stream for RoundRobinPipeConnectorStream<T>
where
    T: Debug + Send + 'static,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let g = self.buffer.state.receiver_queues.read();
        let inner = g
            .get(&self.id)
            .expect("should exist as was inserted in connector creation");
        let mut m_g = inner.write();
        if let Some(item) = m_g.queue.pop_front() {
            if m_g.queue.len() < self.buffer.state.capacity {
                self.buffer.wake_one_sender();
            }

            Poll::Ready(Some(item))
        } else {
            if let Some(item) = self.try_steal_item() {
                return Poll::Ready(Some(item));
            }

            m_g.recv_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
#[derive(Debug)]
pub struct RoundRobinPipeConnector<T> {
    buffer: SharedRoundRobinBuffer<T>,
}

impl<T> Clone for RoundRobinPipeConnector<T> {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
        }
    }
}

impl<T: Send + 'static> RoundRobinPipeConnector<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: SharedRoundRobinBuffer::new(capacity),
        }
    }

    pub fn sink(&self) -> RoundRobinPipeConnectorSink<T> {
        RoundRobinPipeConnectorSink {
            buffer: self.buffer.clone(),
        }
    }

    pub fn stream(&self) -> RoundRobinPipeConnectorStream<T> {
        let id = self.buffer.state.receiver_id_gen.fetch_add(1, Ordering::SeqCst);
        {
            let mut g = self.buffer.state.receiver_queues.write();
            g.insert(
                id,
                RwLock::new(ReceiverInner {
                    queue: VecDeque::new(),
                    recv_waker: None,
                }),
            );
        }

        RoundRobinPipeConnectorStream {
            id,
            buffer: self.buffer.clone(),
        }
    }
}