// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Imports
//======================================================================================================================

use anyhow::Result;
use http::{Request, Response, StatusCode, Uri, Version};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::str::FromStr;

//======================================================================================================================
// Structures
//======================================================================================================================

pub struct HttpStream {
    stream: TcpStream,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl HttpStream {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }

    pub fn parse_request(&self) -> Result<Request<()>> {
        let mut reader: BufReader<&TcpStream> = BufReader::new(&self.stream);

        let mut request_str: String = String::new();

        if let Err(e) = reader.read_line(&mut request_str) {
            let msg: String = format!("failed to read line (e={:?})", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }

        let request_str: &str = &request_str;
        let mut req: Request<()> = Request::default();

        for line in request_str.lines() {
            if line.starts_with("GET") {
                let uri: Uri = match line.split_whitespace().nth(1) {
                    Some(uri_str) => match Uri::from_str(uri_str) {
                        Ok(uri) => uri,
                        Err(e) => {
                            let msg: String = format!("failed to parse uri (e={:?})", e);
                            log::error!("{}", msg);
                            anyhow::bail!(msg);
                        },
                    },
                    None => {
                        let msg: String = format!("missing uri (line={:?})", line);
                        log::error!("{}", msg);
                        anyhow::bail!(msg);
                    },
                };
                *req.uri_mut() = uri;
            } else if line.starts_with("HTTP") {
                let version: Version = match line.split_whitespace().nth(0) {
                    Some(version_str) => match version_str {
                        "HTTP/1.0" => Version::HTTP_10,
                        "HTTP/1.1" => Version::HTTP_11,
                        _ => {
                            let msg: String = format!("unsupported http version (version={:?})", version_str);
                            log::error!("{}", msg);
                            anyhow::bail!(msg);
                        },
                    },
                    None => {
                        let msg: String = format!("missing version (line={:?})", line);
                        log::error!("{}", msg);
                        anyhow::bail!(msg);
                    },
                };
                *req.version_mut() = version;
            }
        }

        Ok(req)
    }

    pub fn send_response(&self, message: Result<Vec<String>>) -> Result<()> {
        let mut writer: BufWriter<&TcpStream> = BufWriter::new(&self.stream);

        let response: Result<Response<Vec<String>>, http::Error> = match message {
            Ok(message) => Response::builder()
                .version(Version::HTTP_11)
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body(message),
            Err(_) => Response::builder()
                .version(Version::HTTP_11)
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header("Content-Type", "text/plain")
                .body(Vec::default()),
        };

        match response {
            Ok(response) => {
                write!(writer, "{:?} {}\r\n", response.version(), response.status())?;
                for (name, value) in response.headers() {
                    write!(writer, "{}: {}\r\n", name.as_str(), value.to_str()?)?;
                }
                write!(writer, "\r\n")?;
                for line in response.body() {
                    write!(writer, "{}\r\n", line)?;
                }
                if let Err(e) = writer.flush() {
                    let msg: String = format!("failed to flush writer (e={:?})", e);
                    log::warn!("{}", msg);
                }
                Ok(())
            },
            Err(e) => {
                let msg: String = format!("failed to build response (e={:?})", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            },
        }
    }
}
