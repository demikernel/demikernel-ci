// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{job::Job, runner::Runner, task::Task, worker::Worker};
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Barrier, Mutex},
    thread::{self, sleep, ScopedJoinHandle},
    time::Duration,
};

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct Scheduler {
    runners: Mutex<Vec<Mutex<Runner>>>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Scheduler {
    const SLEEP_INTERVAL: u64 = 500;

    pub fn new(runners: Vec<Mutex<Runner>>) -> Self {
        Self {
            runners: Mutex::new(runners),
        }
    }

    pub fn run(&self, job: Job) -> Result<Vec<String>> {
        // Schedule tasks.
        let mut schedule: Vec<Worker> = {
            let barriers: Arc<Vec<Barrier>> = Self::create_barriers(&job.barrier_participants());
            let num_workers: usize = job.num_workers();
            let runners: Vec<Mutex<Runner>> = loop {
                if let Ok(runners) = self.allocate_runners(num_workers) {
                    break runners;
                }

                sleep(Duration::from_millis(Self::SLEEP_INTERVAL));
            };
            let placement: HashMap<usize, String> = self.build_placement(&runners, job.get_task_names());
            Self::schedule_tasks(job, runners, placement, barriers)
        };

        thread::scope(|s| {
            let mut threads = Vec::new();
            log::trace!("spawning {} threads", schedule.len());

            for i in 0..schedule.len() {
                let scheduler_worker: &Worker = &schedule[i];

                let thread: ScopedJoinHandle<Result<(), anyhow::Error>> = s.spawn(move || -> Result<()> {
                    while let Some(job_entry) = scheduler_worker.pop_task()? {
                        match job_entry {
                            Task::Action(mut task) => {
                                scheduler_worker.run(&mut task)?;
                                scheduler_worker.push_task(task)?;
                                continue;
                            },
                            Task::Barrier(_) => {
                                scheduler_worker.wait_others()?;
                            },
                        }
                    }

                    Ok(())
                });

                log::trace!("spawned thread (id={:?})", thread.thread().id());
                threads.push(thread);
            }

            for t in threads {
                if let Err(e) = t.join() {
                    log::error!("failed to join thread (error={:?})", e);
                }
            }
        });

        // Collect outputs.
        let output: Vec<String> = {
            let mut job_output: Vec<String> = Vec::new();
            for scheduler_worker in &schedule {
                if let Ok(worker_output) = scheduler_worker.collect_output() {
                    for line in &worker_output {
                        job_output.push(line.to_string());
                    }
                }
            }
            job_output
        };

        // Return workers to the list of idle workers.
        for worker in &mut schedule {
            match worker.take_runner() {
                Some(runner) => {
                    let worker = match Arc::try_unwrap(runner) {
                        Ok(worker) => worker,
                        Err(_) => {
                            let msg: String = format!("leaking worker");
                            log::warn!("{}", &msg);
                            continue;
                        },
                    };
                    match self.runners.lock() {
                        Ok(mut runners) => runners.push(worker),
                        Err(e) => {
                            let msg: String = format!("failed to lock list of runners (e={:?})", e);
                            log::warn!("{}", &msg);
                        },
                    }
                },
                None => {
                    let msg: String = format!("worker has no runner");
                    log::warn!("{}", &msg);
                },
            }
        }

        Ok(output)
    }

    fn create_barriers(barrier_participants: &Vec<usize>) -> Arc<Vec<Barrier>> {
        let mut barriers = Vec::new();
        for num_participants in barrier_participants {
            barriers.push(Barrier::new(*num_participants));
        }
        Arc::new(barriers)
    }

    fn build_placement(
        &self,
        runners: &Vec<Mutex<Runner>>,
        mut task_queue_keys: Vec<String>,
    ) -> HashMap<usize, String> {
        assert_eq!(
            runners.len(),
            task_queue_keys.len(),
            "number of runners must match number of task queues"
        );

        let mut worker_names: HashMap<usize, String> = HashMap::new();
        for runner in runners {
            if let Ok(runner) = &runner.lock() {
                worker_names.insert(runner.id(), task_queue_keys.pop().unwrap());
            }
        }

        worker_names
    }

    fn allocate_runners(&self, num_workers: usize) -> Result<Vec<Mutex<Runner>>> {
        log::trace!("allocate_runners(): num_workers={}", num_workers);
        // Attempt to lock the list of runners and check if we succeeded.
        match self.runners.lock() {
            // We succeeded to lock the list of runners.
            Ok(mut guard) => {
                if guard.len() < num_workers {
                    let msg: String = format!(
                        "not enough runners available (have={}, need={})",
                        guard.len(),
                        num_workers
                    );
                    log::error!("{}", &msg);
                    return Err(anyhow::anyhow!("{}", &msg));
                }

                let mut workers: Vec<Mutex<Runner>> = Vec::new();
                while let Some(worker) = guard.pop() {
                    workers.push(worker);

                    // Finished allocating all workers.
                    if workers.len() == num_workers {
                        break;
                    }
                }

                Ok(workers)
            },
            // We failed to lock the list of runners.
            Err(e) => {
                // Log an error message and return an error.
                let msg: String = format!("failed to lock list of runners (e={:?})", e);
                log::error!("{}", &msg);
                Err(anyhow::anyhow!("{}", &msg))
            },
        }
    }

    fn schedule_tasks(
        mut job: Job,
        mut runners: Vec<Mutex<Runner>>,
        placement: HashMap<usize, String>,
        barriers: Arc<Vec<Barrier>>,
    ) -> Vec<Worker> {
        // Check if the number of required runners matches the number of allocated runners.
        assert_eq!(
            runners.len(),
            job.num_workers(),
            "number of required runners must match number allocated runners (have={}, need={})",
            runners.len(),
            job.num_workers()
        );

        // Check if the number of allocated runners match the size of the worker/runner placement.
        assert_eq!(
            runners.len(),
            placement.len(),
            "number of allocated runners must match size of worker/runner placement (have={}, need={})",
            runners.len(),
            placement.len()
        );

        let mut worker_id: usize = 0;
        let mut workers: Vec<Worker> = Vec::new();
        while let Some(runner) = runners.pop() {
            let runner_id: usize = runner.lock().unwrap().id();
            let worker_name: &String = placement
                .get(&runner_id)
                .expect("numbers of allocated runners should match the number of required workers");
            let runner: Arc<Mutex<Runner>> = Arc::new(runner);
            let worker: Worker = match Worker::new(runner, &worker_name, &mut job, barriers.clone()) {
                Ok(worker) => worker,
                Err(e) => {
                    let msg: String = format!("failed to create worker (e={:?})", e);
                    log::error!("{}", &msg);
                    panic!("{}", &msg);
                },
            };
            workers.push(worker);
        }

        workers
    }
}

unsafe impl Sync for Scheduler {}
