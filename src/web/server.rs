// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use super::stream::HttpStream;
use anyhow::{Error, Result};
use http::Request;
use std::net::TcpListener;
use std::thread::{self, JoinHandle};

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct HttpServer {
    pub listener: TcpListener,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl HttpServer {
    const THREAD_MAX: usize = 4;

    pub fn new(addr: &str) -> Result<Self> {
        log::info!("bind to address={:?}", addr);
        let listener: TcpListener = match TcpListener::bind(addr) {
            Ok(listener) => listener,
            Err(e) => {
                let msg: String = format!("failed to bind (addr={:?}, e={:?})", &addr, e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        };
        Ok(Self { listener })
    }

    pub fn run<F>(&self, dispatcher: F)
    where
        F: FnOnce(Request<()>) -> Result<Vec<String>> + Sync + std::marker::Send + 'static + Clone,
    {
        let mut threads = Vec::new();
        for stream in self.listener.incoming() {
            match stream {
                Ok(_) => {
                    let dispatcher_ = dispatcher.clone();
                    let thread: JoinHandle<Result<(), Error>> = thread::spawn(move || {
                        let server: HttpStream = HttpStream::new(stream?);
                        let request: Request<()> = server.parse_request()?;
                        let result: Result<Vec<String>, Error> = dispatcher_(request);
                        server.send_response(result)?;
                        Ok(())
                    });
                    threads.push(thread);
                },
                Err(_) => {
                    log::error!("failed to accept connection")
                },
            }

            // Join threads.
            if threads.len() < Self::THREAD_MAX {
                continue;
            }

            while let Some(thread) = threads.pop() {
                if let Err(e) = thread.join() {
                    log::error!("failed to join thread: {:?}", e);
                }
            }
        }
    }
}
