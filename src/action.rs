// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Structures
//======================================================================================================================

#[derive(Debug)]
pub struct Action {
    /// Name of this action.
    name: String,
    /// List of commands to be executed.
    commands: Vec<String>,
    /// Worker on which this task should run.
    runs_on: String,
    /// Output of this task.
    output: Option<Vec<String>>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Action {
    /// Instantiates a new [Action].
    pub fn new(name: &str, commands: Vec<String>, runs_on: &str) -> Self {
        log::trace!("action: commands={:?}, runs_on={:?}", commands, runs_on);

        Self {
            name: name.to_string(),
            commands,
            runs_on: runs_on.to_string(),
            output: None,
        }
    }

    /// Returns the list of commands of the target [Action].
    pub fn commands(&self) -> &Vec<String> {
        &self.commands
    }

    /// Returns the output of the target [Action].
    pub fn output(&self) -> &Option<Vec<String>> {
        &self.output
    }

    /// Returns the worker on which the target [Action] should run.
    pub fn runs_on(&self) -> &str {
        &self.runs_on
    }

    /// returns the name of the target [Action].
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the output of the target [Action].
    pub fn set_output(&mut self, output: Vec<String>) {
        self.output = Some(output);
    }
}
