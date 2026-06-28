use std::sync::{Arc, Mutex};

use surgeist_task::{
    CancellationToken, CoalescingKey, TaskAttemptId, TaskContext, TaskEvent, TaskEventSink,
    TaskEventSinkError, TaskId, TaskKey, TaskRunnable, TaskScope, TaskSpawnRequest,
};

#[derive(Clone, Debug, Default)]
struct ExampleEventLog {
    events: Arc<Mutex<Vec<TaskEvent<String>>>>,
}

impl ExampleEventLog {
    fn sink(&self) -> ExampleEventSink {
        ExampleEventSink {
            events: Arc::clone(&self.events),
        }
    }

    fn events(&self) -> Vec<TaskEvent<String>> {
        self.events
            .lock()
            .expect("example event log mutex poisoned")
            .clone()
    }
}

#[derive(Debug)]
struct ExampleEventSink {
    events: Arc<Mutex<Vec<TaskEvent<String>>>>,
}

impl TaskEventSink<String> for ExampleEventSink {
    fn emit(&self, event: TaskEvent<String>) -> Result<(), TaskEventSinkError> {
        self.events
            .lock()
            .expect("example event sink mutex poisoned")
            .push(event);
        Ok(())
    }
}

fn main() {
    let task_id = TaskId::from_u64(1);
    let attempt_id = TaskAttemptId::from_u64(1);
    let cancellation = CancellationToken::new();
    let event_log = ExampleEventLog::default();

    let request = TaskSpawnRequest::builder(task_id, attempt_id)
        .key(TaskKey::try_new("import:orders").expect("static task key is valid"))
        .scope(TaskScope::try_workspace("workspace-alpha").expect("static task scope is valid"))
        .cancellation(cancellation.clone())
        .sink(event_log.sink())
        .input("orders.csv".to_owned())
        .blocking_fn(|path, context| {
            if context.is_cancelled() {
                return Ok(());
            }

            context.progress_units(
                CoalescingKey::try_new("rows").expect("static progress key is valid"),
                10,
                Some(100),
            )?;
            context.output(format!("loaded {path}"))?;
            Ok(())
        })
        .build()
        .expect("task spawn request is complete");

    let (task_id, attempt_id, _key, _scope, _policy, cancellation, input, runnable, sink) =
        request.into_parts();
    let context = TaskContext::new(task_id, attempt_id, cancellation.view(), sink);

    match runnable {
        TaskRunnable::Blocking(job) => job.run(input, context).expect("task job succeeds"),
        TaskRunnable::Async(_) => unreachable!("example request uses a blocking runnable"),
    }

    let events = event_log.events();
    assert_eq!(events.len(), 2);
    println!("emitted {} task events", events.len());
}
