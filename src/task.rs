// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::action::Action;
use std::collections::VecDeque;

//======================================================================================================================
// Structures
//======================================================================================================================

#[derive(Debug)]
pub enum Task {
    Action(Action),
    Barrier(usize),
}

#[derive(Default)]
pub struct TaskQueue {
    tasks: VecDeque<Task>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl TaskQueue {
    pub fn push_back(&mut self, job_entry: Task) {
        self.tasks.push_back(job_entry);
    }

    pub fn pop_front(&mut self) -> Option<Task> {
        self.tasks.pop_front()
    }

    pub fn tasks(&self) -> &VecDeque<Task> {
        &self.tasks
    }
}
