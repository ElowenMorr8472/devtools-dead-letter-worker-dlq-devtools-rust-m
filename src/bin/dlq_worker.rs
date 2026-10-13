use dead_letter_worker::{
    infrai_queue::{InfraiError, InfraiQueue},
    poison_job::{BuildEvent, EventError, JobDecision},
};
use std::{
    future::Future,
    pin::pin,
    process::ExitCode,
    task::{Context, Poll, Wake, Waker},
};

const SOURCE_QUEUE: &str = "release-build-events";
const DEAD_LETTER_QUEUE: &str = "release-build-dead-letters";

#[derive(Debug)]
enum WorkerError {
    Api(InfraiError),
    Event(EventError),
}

impl From<InfraiError> for WorkerError {
    fn from(value: InfraiError) -> Self {
        Self::Api(value)
    }
}
impl From<EventError> for WorkerError {
    fn from(value: EventError) -> Self {
        Self::Event(value)
    }
}

async fn run_batch() -> Result<usize, WorkerError> {
    let infrai = InfraiQueue::from_env()?;
    let mut moved = 0;
    for message in infrai.consume(SOURCE_QUEUE, 10, 60).await? {
        let event = BuildEvent::from_payload(&message.payload)?;
        match event.decide(3) {
            JobDecision::Retry => println!("retry pending: {}", message.message_id),
            JobDecision::DeadLetter(diagnostic) => {
                let key = format!(
                    "dead-letter-{}-{}",
                    diagnostic.build_id, diagnostic.attempts
                );
                infrai
                    .publish(DEAD_LETTER_QUEUE, &diagnostic.to_json(), &key)
                    .await?;
                infrai.ack(SOURCE_QUEUE, &message.message_id).await?;
                println!(
                    "dead-lettered: {} {}",
                    diagnostic.build_id, diagnostic.operation
                );
                moved += 1;
            }
        }
    }
    Ok(moved)
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(std::sync::Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn main() -> ExitCode {
    match block_on(run_batch()) {
        Ok(moved) => {
            println!("batch complete: {moved} moved");
            ExitCode::SUCCESS
        }
        Err(WorkerError::Api(error)) => {
            eprintln!("queue error: {error}");
            ExitCode::FAILURE
        }
        Err(WorkerError::Event(error)) => {
            eprintln!("event error: {error}");
            ExitCode::FAILURE
        }
    }
}
