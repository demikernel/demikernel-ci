// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Modules
//======================================================================================================================

mod action;
mod args;
mod config;
mod credentials;
mod job;
mod runner;
mod scheduler;
mod task;
mod web;
mod worker;

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::args::ProgramArguments;
use crate::credentials::Credentials;
use ::flexi_logger::Logger;
use ::std::sync::Once;
use anyhow::Result;
use config::Config;
use http::Request;
use job::Job;
use runner::Runner;
use scheduler::Scheduler;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use web::server::HttpServer;

//======================================================================================================================
// Static Variables
//======================================================================================================================

/// Guardian to the logging initialize function.
static INIT_LOG: Once = Once::new();

//======================================================================================================================
// Standalone Functions
//======================================================================================================================

fn main() -> Result<()> {
    INIT_LOG.call_once(|| match Logger::try_with_env() {
        Ok(logger) => {
            if let Err(_) = logger.start() {
                panic!("failed to initialize logger")
            }
        },
        Err(_) => {
            panic!("malformed RUST_LOG")
        },
    });

    let args: ProgramArguments = ProgramArguments::new(
        "demikernel-ci",
        "Pedro Henrique Penna <ppenna@microsoft.com>",
        "CI Orchestrator for Demikernel",
    )?;

    let credentials: Credentials = Credentials::new(args.username(), args.public_key_path(), args.private_key_path());
    let config: Config = Config::new(args.config_file())?;
    let web_server: HttpServer = HttpServer::new(&config.addr()?)?;
    let runners: Vec<Mutex<Runner>> = config.get_workers(&credentials)?;
    let scheduler: Arc<Scheduler> = Arc::new(Scheduler::new(runners));
    let job_home: String = config.jobs_home();
    let env_var_prefix: String = Config::env_var_prefix();

    // Request dispatcher.
    let dispatcher = |request: Request<()>| -> Result<Vec<String>> {
        match request.uri().path() {
            // Run a job.
            "/run" => run_job(env_var_prefix, job_home, scheduler, request),
            // Unsupported.
            unsupported => {
                let message: String = format!("unsupported trigger (trigger={:?})", unsupported);
                log::error!("{}", message);
                Err(anyhow::anyhow!("{}", message))
            },
        }
    };

    web_server.run(dispatcher);

    Ok(())
}

fn run_job(
    env_var_prefix: String,
    job_home: String,
    scheduler: Arc<Scheduler>,
    request: Request<()>,
) -> Result<Vec<String>> {
    log::trace!("run_job(): uri={}", request.uri());
    match request.uri().query() {
        Some(parameters) => {
            let parameters: HashMap<String, String> = parse_job_parameters(parameters);

            if parameters.is_empty() {
                let message: String = format!("malformed query");
                log::error!("{}", message);
                return Err(anyhow::anyhow!("{}", message));
            }

            let job_name: String = match parameters.get("JOB") {
                Some(job_name) => job_name.to_string(),
                None => {
                    let message: String = format!("missing job name");
                    log::error!("{}", message);
                    return Err(anyhow::anyhow!("{}", message));
                },
            };

            // Pre-append the environment variable prefix to each key in the parameters.
            let mut env: HashMap<String, String> = HashMap::new();
            for (key, value) in parameters {
                let new_key = format!("{}{}", env_var_prefix, key);
                env.insert(new_key, value);
            }

            let job_path: String = format!("{}/{}", job_home, job_name);
            match Job::new(&job_path, env) {
                Ok(job) => scheduler.run(job),
                Err(e) => Err(e),
            }
        },
        None => {
            let message: String = format!("missing query");
            log::error!("{}", message);
            Err(anyhow::anyhow!("{}", message))
        },
    }
}

fn parse_job_parameters(query: &str) -> HashMap<String, String> {
    // Create an empty vector to store the results
    let mut result: HashMap<String, String> = HashMap::new();
    // Split the query string by the '&' character and iterate over the substrings
    for pair in query.split('&') {
        // Split each substring by the '=' character and collect the parts into a vector
        let parts: Vec<&str> = pair.split('=').collect();
        // If the vector has exactly two elements, push them as a tuple into the result vector
        if parts.len() == 2 {
            log::trace!("inserting query pair (key={}, value={})", parts[0], parts[1]);
            if result
                .insert(parts[0].to_string().to_uppercase(), parts[1].to_string())
                .is_some()
            {
                log::warn!("duplicate parameter (key={})", parts[0]);
            }
        }
    }
    // Return the result vector
    result
}
