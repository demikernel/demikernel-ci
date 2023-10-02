// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{
    action::Action,
    task::{Task, TaskQueue},
};
use ::anyhow::Result;
use ::yaml_rust::{Yaml, YamlLoader};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Read,
};

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct Job {
    env: HashMap<String, String>,
    tasks_queues: HashMap<String, TaskQueue>,
    barrier_participants: Vec<usize>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Job {
    const JOB_ENTRY_NAME: &'static str = "job";
    const ACTION_ENTRY_NAME: &'static str = "action";
    const BARRIER_ENTRY_NAME: &'static str = "barrier";
    const RUNS_ON_ENTRY_NAME: &'static str = "runs-on";
    const COMMANDS_ENTRY_NAME: &'static str = "commands";

    pub fn new(job_path: &str, parameters: HashMap<String, String>) -> Result<Self> {
        log::trace!("job: path={}, env={:?}", job_path, parameters);
        let mut job_s: String = String::new();
        File::open(job_path)?.read_to_string(&mut job_s)?;

        let yaml: Vec<Yaml> = YamlLoader::load_from_str(&job_s)?;
        let mut job_entries: VecDeque<Task> = Self::parse(&yaml)?;

        let mut tasks: HashMap<String, TaskQueue> = HashMap::new();
        let mut barrier_participants = 0;
        let mut barrier_participants_ = Vec::new();
        loop {
            let job_entry: Option<Task> = job_entries.pop_front();

            match job_entry {
                Some(Task::Action(task)) => {
                    barrier_participants += 1;
                    // Insert task on the queue of the worker on which it should run.
                    let runs_on: String = task.runs_on().to_string();

                    let task_queue = tasks.entry(runs_on).or_insert_with(|| TaskQueue::default());
                    task_queue.push_back(Task::Action(task));
                },
                Some(Task::Barrier(_)) => {
                    // Insert barrier in all work queues.
                    for (_, task_queue) in tasks.iter_mut() {
                        task_queue.push_back(Task::Barrier(barrier_participants));
                    }
                    barrier_participants_.push(barrier_participants);
                    barrier_participants = 0;
                },
                None => break,
            }
        }

        Ok(Self {
            env: parameters,
            tasks_queues: tasks,
            barrier_participants: barrier_participants_,
        })
    }

    // Return the set of tasks that are associated to a given worker.
    pub fn get_worker_tasks(&mut self, worker_name: &str) -> Option<TaskQueue> {
        self.tasks_queues.remove(worker_name)
    }

    pub fn barrier_participants(&self) -> &Vec<usize> {
        &self.barrier_participants
    }

    pub fn num_workers(&self) -> usize {
        self.tasks_queues.len()
    }

    pub fn get_task_names(&self) -> Vec<String> {
        self.tasks_queues.keys().cloned().collect()
    }

    /// Parses a job file.
    fn parse(docs: &Vec<Yaml>) -> Result<VecDeque<Task>> {
        // Parse job entry.
        let doc: &Yaml = &docs[0];
        let job: &Vec<Yaml> = match doc[Self::JOB_ENTRY_NAME].as_vec() {
            Some(job) => job,
            None => {
                let msg: String = format!("missing {} entry", Self::JOB_ENTRY_NAME);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };

        // Parse task entries.
        let mut tasks: VecDeque<Task> = VecDeque::new();
        for task in job {
            if let Some(entry) = task.as_hash() {
                // Check if we need to parse an action entry.
                if let Some(action_entry) = entry.get(&Yaml::from_str(Self::ACTION_ENTRY_NAME)) {
                    // Parse action name.
                    let name: String = match action_entry.as_str() {
                        Some(action_entry_str) => action_entry_str.to_string(),
                        None => {
                            let msg: String = format!("failed to parse {} entry", Self::ACTION_ENTRY_NAME);
                            log::error!("{}", msg);
                            anyhow::bail!(msg);
                        },
                    };

                    // Parse runs-on entry.
                    let runs_on: String = match entry.get(&Yaml::from_str(Self::RUNS_ON_ENTRY_NAME)) {
                        Some(runs_on_entry) => match runs_on_entry.as_str() {
                            Some(runs_on_entry_str) => runs_on_entry_str.to_string(),
                            None => {
                                let msg: String = format!("failed to parse {} entry", Self::RUNS_ON_ENTRY_NAME);
                                log::error!("{}", msg);
                                anyhow::bail!(msg);
                            },
                        },
                        None => {
                            let msg: String = format!("missing {} entry", Self::RUNS_ON_ENTRY_NAME);
                            log::error!("{}", msg);
                            anyhow::bail!(msg);
                        },
                    };

                    // Parse commands entry.
                    let commands: Vec<String> = match entry.get(&Yaml::from_str(Self::COMMANDS_ENTRY_NAME)) {
                        Some(commands_entry) => match commands_entry.as_vec() {
                            Some(commands_entry_vec) => {
                                let mut commands: Vec<String> = Vec::default();
                                for command in commands_entry_vec {
                                    match command.as_str() {
                                        Some(command_str) => commands.push(command_str.to_string()),
                                        None => {
                                            let msg: String =
                                                format!("failed to parse {} entry", Self::COMMANDS_ENTRY_NAME);
                                            log::error!("{}", msg);
                                            anyhow::bail!(msg);
                                        },
                                    }
                                }
                                commands
                            },
                            None => {
                                let msg: String = format!("failed to parse {} entry", Self::COMMANDS_ENTRY_NAME);
                                log::error!("{}", msg);
                                anyhow::bail!(msg);
                            },
                        },
                        None => {
                            let msg: String = format!("missing {} entry", Self::COMMANDS_ENTRY_NAME);
                            log::error!("{}", msg);
                            anyhow::bail!(msg);
                        },
                    };

                    // Create action and insert it into the list of tasks.
                    let action: Action = Action::new(&name, commands, &runs_on);
                    tasks.push_back(Task::Action(action));
                }
                // Check if we need to parse a barrier entry.
                else if entry.contains_key(&Yaml::from_str(Self::BARRIER_ENTRY_NAME)) {
                    // Create barrier and insert it into the list of tasks.
                    tasks.push_back(Task::Barrier(0));
                }
                // Skip unsupported entries.
                else {
                    log::warn!("skipping entry (entry={:?})", entry);
                }
            }
        }

        Ok(tasks)
    }

    /// Returns the environment variables that should be set for the job.
    pub fn env(&self) -> &HashMap<String, String> {
        &self.env
    }
}
