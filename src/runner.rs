// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use crate::{action::Action, credentials::Credentials};
use anyhow::{Error, Result};
use ssh2::{Channel, Session, Stream};
use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
    net::TcpStream,
    path::Path,
};

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct Runner {
    addr: String,
    local_addr: String,
    session: Session,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Runner {
    const KEEP_ALIVE_INTERVAL: u32 = 5;

    /// Instantiates a new [Runner] object.
    pub fn new(hostname: &str, port: u16, local_addr: &str, credentials: &Credentials) -> Result<Self> {
        // Create a TCP stream to connect to the server.
        let addr: String = format!("{}:{}", hostname, port);
        let tcp: TcpStream = match TcpStream::connect(&addr) {
            Ok(tcp) => tcp,
            Err(e) => {
                let msg: String = format!("failed to connect (addr={:?}, e={:?})", &addr, e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };

        if let Err(e) = tcp.set_read_timeout(None) {
            let msg: String = format!("failed to set read timeout (e={:?})", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }

        // Create a new SSH session from the TCP stream.
        let mut session: Session = match Session::new() {
            Ok(session) => session,
            Err(e) => {
                let msg: String = format!("failed to create session (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };
        session.set_tcp_stream(tcp);
        if let Err(e) = session.handshake() {
            let msg: String = format!("failed to handshake (e={:?})", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }
        session.set_blocking(true);
        session.set_compress(true);
        session.set_timeout(0);

        if session.timeout() != 0 {
            let msg: String = format!("failed to set timeout");
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }

        session.set_keepalive(true, Self::KEEP_ALIVE_INTERVAL);

        // Use username and public key to authenticate.
        let pubkey: &Path = Path::new(credentials.public_key_path());
        let privatekey: &Path = Path::new(credentials.private_key_path());
        let username: &str = credentials.username();
        match session.userauth_pubkey_file(username, Some(pubkey), privatekey, None) {
            Ok(()) => {},
            Err(e) => {
                let msg: String = format!("failed to authenticate (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        }

        // Check if authentication failed
        if !session.authenticated() {
            let msg: String = format!("authentication failed");
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }

        Ok(Self {
            addr,
            local_addr: local_addr.to_string(),
            session,
        })
    }

    pub fn run(&mut self, action: &Action, env: &HashMap<String, String>) -> Result<Vec<String>> {
        let commands: &Vec<String> = action.commands();
        let mut cmdline: String = String::new();

        log::trace!("run: addr={:?}, command={:?}", self.addr, commands);

        // Concatenate all commands.
        for command in commands {
            cmdline.push_str(command);

            // Do not concatenate if last command.
            // Note that it is safe to call expect() because we are iterating
            // over the commands list, and thus it cannot be empty.
            if command != commands.last().expect("commands list cannot be empty") {
                cmdline.push_str(" &&");
            }
        }

        // Open a session-based channel for running a command.
        let mut channel: Channel = match self.session.channel_session() {
            Ok(channel) => channel,
            Err(e) => {
                let msg: String = format!("failed to open session-based channel (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };

        // Set environment variables.
        for (key, value) in env {
            if key.to_lowercase() != "job" {
                if let Err(e) = channel.setenv(key, value) {
                    let msg: String = format!("failed to set environment variable (key={:?}, e={:?})", key, e);
                    log::warn!("{}", msg);
                }
            }
        }

        //==========================================================================
        // NOTE: from this point on, we must close the channel before returning.
        //==========================================================================

        // Execute the command and parse result.
        let result: Result<Vec<String>, Error> = self.do_run(&mut channel, &cmdline);

        // Close the session-based channel and check if we succeeded.
        match channel.close() {
            // We succeed to close the session-based channel.
            Ok(()) => {
                // Wait for the channel to close and check if we succeeded.
                if let Err(e) = channel.wait_close() {
                    // We failed, thus log a warning message and keep going.
                    let msg: String = format!(
                        "failed to wait for channel to close (e={:?}, eof={:?})",
                        e,
                        channel.eof()
                    );
                    log::warn!("{}", msg);
                }
            },
            // We failed to close the session-based channel.
            Err(e) => {
                // Log a warning message and keep going.
                let msg: String = format!("failed to close channel (e={:?})", e);
                log::warn!("{}", msg);
            },
        }

        result
    }

    fn read_inboud_stream(stream: &mut Stream) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        loop {
            let mut buf: Vec<u8> = vec![0; 1];
            match stream.read_exact(&mut buf) {
                Ok(()) => {
                    // convert byte to char.
                    bytes.push(buf[0]);
                },
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) if e.kind() == ErrorKind::TimedOut => break,
                Err(e) => log::warn!("failed to read from channel (e={:?})", e),
            }
        }
        bytes
    }

    fn process_bytes(stream_name: &str, bytes: Vec<u8>) -> Vec<String> {
        let mut output: Vec<String> = Vec::new();
        // Construct string from bytes.
        let s: String = String::from_utf8(bytes).unwrap();

        output.push(s);

        // Break output into lines.
        let output: Vec<String> = output
            .iter()
            .map(|s| s.split('\n').map(|s| s.to_string()).collect::<Vec<String>>())
            .flatten()
            .collect();

        // Remove empty lines.
        let output: Vec<String> = output.iter().filter(|s| !s.is_empty()).map(|s| s.to_string()).collect();

        // Pre-append stream name to each line.
        let output: Vec<String> = output.iter().map(|s| format!("[{}] {}", stream_name, s)).collect();
        output
    }

    fn do_run(&mut self, channel: &mut Channel, cmdline: &str) -> Result<Vec<String>> {
        // Execute the command and check if we succeeded.
        match channel.exec(&cmdline) {
            // We succeed to execute the command.
            Ok(()) => {
                let mut output: Vec<String> = Vec::default();

                loop {
                    let mut stdout_stream: Stream = channel.stream(0);
                    let stdout_bytes: Vec<u8> = Self::read_inboud_stream(&mut stdout_stream);
                    let mut stderr_stream: Stream = channel.stderr();
                    let stderr_bytes: Vec<u8> = Self::read_inboud_stream(&mut stderr_stream);

                    // Process stdout.
                    if stdout_bytes.len() > 0 {
                        let mut stdout: Vec<String> = Self::process_bytes("stdout", stdout_bytes);
                        output.append(&mut stdout);
                    }

                    // Process stderr.
                    if stderr_bytes.len() > 0 {
                        let mut stderr: Vec<String> = Self::process_bytes("stderr", stderr_bytes);
                        output.append(&mut stderr);
                    }

                    if channel.eof() {
                        break;
                    }

                    if output.is_empty() {
                        let msg: String = format!("unexpected error");
                        log::error!("{}", msg);
                        anyhow::bail!(msg);
                    }
                }

                Ok(output)
            },
            // We did not succeeded to run the command.
            Err(e) => {
                // Log an error message and return an error.
                let msg: String = format!("failed to execute command (e={:?})", e);
                log::error!("{}", msg);
                Err(anyhow::anyhow!(msg))
            },
        }
    }

    /// Retrieves the local address of the target [Runner].
    pub fn local_addr(&self) -> &str {
        &self.local_addr
    }
}
