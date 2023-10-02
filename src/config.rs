// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{credentials::Credentials, runner::Runner};
use ::std::{fs::File, io::Read};
use ::yaml_rust::{Yaml, YamlLoader};
use anyhow::Result;
use std::sync::Mutex;

//======================================================================================================================
// Structures
//======================================================================================================================

/// Configuration file.
pub struct Config {
    yaml: Vec<Yaml>,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Config {
    pub const ENV_VAR_PREFIX: &'static str = "DEMIKERNEL_";

    /// Reads a configuration file into a [Config] object.
    pub fn new(config_path: &str) -> Result<Self> {
        let mut config_s: String = String::new();

        match File::open(config_path) {
            Ok(mut config_file) => {
                if let Err(e) = config_file.read_to_string(&mut config_s) {
                    let msg: String = format!("failed to read config file (e={:?})", e);
                    log::error!("{}", msg);
                    anyhow::bail!(msg);
                }
            },
            Err(e) => {
                let msg: String = format!("failed to open config file (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };

        let yaml: Vec<Yaml> = match YamlLoader::load_from_str(&config_s) {
            Ok(yaml) => yaml,
            Err(e) => {
                let msg: String = format!("failed to parse config file (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };

        Ok(Self { yaml })
    }

    /// Retrieves the list of workers from target [Config] object.
    pub fn get_workers(&self, credentials: &Credentials) -> Result<Vec<Mutex<Runner>>> {
        let mut workers: Vec<Mutex<Runner>> = Vec::new();
        for c in &self.yaml {
            if let Some(workers_config) = c["workers"].as_vec() {
                for worker_config in workers_config {
                    let hostname: String = match worker_config["hostname"].as_str() {
                        Some(hostname) => hostname.to_string(),
                        None => anyhow::bail!("missing hostname"),
                    };

                    let port: u16 = worker_config["port"]
                        .as_i64()
                        .ok_or(anyhow::anyhow!("failed to parse port number"))?
                        as u16;

                    let local_addr: String = match worker_config["local-address"].as_str() {
                        Some(local_addr) => local_addr.to_string(),
                        None => anyhow::bail!("missing local_addr"),
                    };

                    let worker: Runner = Runner::new(&hostname, port, &local_addr, credentials)?;
                    workers.push(Mutex::new(worker));
                }
            }
        }

        Ok(workers)
    }

    /// Retrieves the server address from target [Config] object.
    pub fn addr(&self) -> Result<String> {
        for c in &self.yaml {
            if let Some(server_config) = c["server"].as_vec() {
                for c in server_config {
                    if let Some(bind_config) = c["bind"].as_hash() {
                        let address: Option<String> = match bind_config.get(&Yaml::from_str("address")) {
                            Some(address_entry) => match address_entry.as_str() {
                                Some(address) => Some(address.to_string()),
                                None => {
                                    let msg: String = format!("failed to parse bind address");
                                    log::error!("{}", msg);
                                    anyhow::bail!(msg);
                                },
                            },
                            None => None,
                        };

                        let port: Option<String> = match bind_config.get(&Yaml::from_str("port")) {
                            Some(port_entry) => match port_entry.as_i64() {
                                Some(port) => Some(port.to_string()),
                                None => {
                                    let msg: String = format!("failed to parse bind port");
                                    log::error!("{}", msg);
                                    anyhow::bail!(msg);
                                },
                            },
                            None => None,
                        };

                        match (address, port) {
                            (Some(address), Some(port)) => return Ok(format!("{}:{}", address, port).to_string()),
                            _ => {
                                let msg: String = format!("malformed bind address");
                                log::error!("{}", msg);
                                return Err(anyhow::anyhow!(msg));
                            },
                        }
                    }
                }
            }
        }

        let msg: String = format!("missing bind address");
        log::error!("{}", msg);
        Err(anyhow::anyhow!(msg))
    }

    /// Retrieves the location of the jobs directory from target [Config] object.
    pub fn jobs_home(&self) -> String {
        "jobs".to_string()
    }

    /// Retrieves the prefix for environment variables from target [Config] object.
    pub fn env_var_prefix() -> String {
        Self::ENV_VAR_PREFIX.to_string()
    }
}
