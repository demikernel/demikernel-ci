// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{
    action::Action,
    job::Job,
    runner::Runner,
    task::{Task, TaskQueue},
};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Barrier, Mutex},
};

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct Worker {
    env: HashMap<String, String>,
    runner: Option<Arc<Mutex<Runner>>>,
    scheduled_tasks: Arc<Mutex<TaskQueue>>,
    completed_tasks: Arc<Mutex<TaskQueue>>,
    barriers: Arc<Vec<Barrier>>,
    next_barrier: Arc<Mutex<usize>>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Worker {
    pub fn new(
        runner: Arc<Mutex<Runner>>,
        runner_name: &str,
        job: &mut Job,
        barriers: Arc<Vec<Barrier>>,
    ) -> Result<Self> {
        let env = job.env().clone();
        let tasks: TaskQueue = match job.get_worker_tasks(runner_name) {
            Some(tasks) => tasks,
            None => {
                let msg: String = format!("no tasks for runner {}", runner_name);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };
        Ok(Self {
            env,
            runner: Some(runner),
            scheduled_tasks: Arc::new(Mutex::new(tasks)),
            completed_tasks: Arc::new(Mutex::new(TaskQueue::default())),
            barriers: barriers.clone(),
            next_barrier: Arc::new(Mutex::new(0)),
        })
    }

    pub fn pop_task(&self) -> Result<Option<Task>> {
        match self.scheduled_tasks.lock() {
            Ok(mut schedule_tasks) => Ok(schedule_tasks.pop_front()),
            Err(e) => {
                let msg: String = format!("failed to lock queue of scheduled tasks (e={:?})", e);
                log::error!("{}", msg);
                Err(anyhow::anyhow!("{}", msg))
            },
        }
    }

    pub fn push_task(&self, task: Action) -> Result<()> {
        match self.completed_tasks.lock() {
            Ok(mut completed_tasks) => completed_tasks.push_back(Task::Action(task)),
            Err(e) => {
                let msg: String = format!("failed to lock queue of completed tasks (e={:?})", e);
                log::error!("{}", msg);
            },
        }

        Ok(())
    }

    pub fn wait_others(&self) -> Result<()> {
        match self.next_barrier.lock() {
            Ok(mut next_barrier) => {
                self.barriers[*next_barrier].wait();
                *next_barrier += 1;
                Ok(())
            },
            Err(e) => {
                let msg: String = format!("failed to lock next barrier (e={:?})", e);
                log::error!("{}", msg);
                Err(anyhow::anyhow!("{}", msg))
            },
        }
    }

    pub fn run(&self, action: &mut Action) -> Result<()> {
        if let Some(runner) = &self.runner {
            match runner.lock() {
                Ok(mut runner) => match runner.run(action, &self.env) {
                    Ok(result) => {
                        // Pre-append runner name and worker name to each line of the output.
                        let result: Vec<String> = result
                            .iter()
                            .map(|s| format!("[{}][{}]{}", action.runs_on(), action.name(), s))
                            .collect();
                        action.set_output(result);
                        Ok(())
                    },
                    Err(e) => {
                        let msg: String = format!("failed to run task (e={:?})", e);
                        log::error!("{}", msg);
                        Err(anyhow::anyhow!("{}", msg))
                    },
                },
                Err(e) => {
                    let msg: String = format!("failed to lock runner (e={:?})", e);
                    log::error!("{}", msg);
                    Err(anyhow::anyhow!("{}", msg))
                },
            }
        } else {
            anyhow::bail!("runner is None");
        }
    }

    pub fn take_runner(&mut self) -> Option<Arc<Mutex<Runner>>> {
        self.runner.take()
    }

    pub fn collect_output(&self) -> Result<Vec<String>> {
        match self.completed_tasks.lock() {
            Ok(completed_tasks) => {
                let mut output: Vec<String> = Vec::default();
                for task in completed_tasks.tasks() {
                    if let Task::Action(task) = task {
                        if let Some(lines) = task.output() {
                            for line in lines {
                                output.push(line.clone());
                            }
                        }
                    }
                }
                Ok(output)
            },
            Err(e) => {
                let msg: String = format!("failed to lock queue of completed tasks (e={:?})", e);
                log::error!("{}", msg);
                Err(anyhow::anyhow!("{}", msg))
            },
        }
    }
}
