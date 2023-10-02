// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use anyhow::Result;
use clap::{Arg, ArgMatches, Command};

//======================================================================================================================
// Structures
//======================================================================================================================

/// Program Arguments
#[derive(Debug)]
pub struct ProgramArguments {
    /// Location for configuration file.
    config_file: String,
    /// Username for authentication.
    username: String,
    /// Location for public key.
    public_key_path: String,
    /// Location for private key.
    private_key_path: String,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl ProgramArguments {
    /// Parses program arguments from the command line interface.
    pub fn new(app_name: &'static str, app_author: &'static str, app_about: &'static str) -> Result<Self> {
        let matches: ArgMatches = Command::new(app_name)
            .author(app_author)
            .about(app_about)
            .arg(
                Arg::new("config-file")
                    .long("config-file")
                    .value_parser(clap::value_parser!(String))
                    .required(true)
                    .value_name("path")
                    .help("Sets location for configuration file"),
            )
            .arg(
                Arg::new("username")
                    .long("username")
                    .value_parser(clap::value_parser!(String))
                    .required(true)
                    .value_name("string")
                    .help("Sets username for authentication"),
            )
            .arg(
                Arg::new("public-key")
                    .long("public-key")
                    .value_parser(clap::value_parser!(String))
                    .required(true)
                    .value_name("path")
                    .help("Sets location for public key"),
            )
            .arg(
                Arg::new("private-key")
                    .long("private-key")
                    .value_parser(clap::value_parser!(String))
                    .required(true)
                    .value_name("path")
                    .help("Sets location for private key"),
            )
            .get_matches();

        let config_file: String = matches
            .get_one::<String>("config-file")
            .ok_or(anyhow::anyhow!("Missing configuration file"))?
            .to_string();
        let username: String = matches
            .get_one::<String>("username")
            .ok_or(anyhow::anyhow!("Missing username"))?
            .to_string();
        let private_key_path: String = matches
            .get_one::<String>("private-key")
            .ok_or(anyhow::anyhow!("Missing private key"))?
            .to_string();
        let public_key_path: String = matches
            .get_one::<String>("public-key")
            .ok_or(anyhow::anyhow!("Missing public key"))?
            .to_string();

        Ok(Self {
            config_file,
            username,
            public_key_path,
            private_key_path,
        })
    }

    /// Returns the location for the configuration file.
    pub fn config_file(&self) -> &str {
        &self.config_file
    }

    /// Returns the username for authentication.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the location for the public key.
    pub fn public_key_path(&self) -> &str {
        &self.public_key_path
    }

    /// Returns the location for the private key.
    pub fn private_key_path(&self) -> &str {
        &self.private_key_path
    }
}
