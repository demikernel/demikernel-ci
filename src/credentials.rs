// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//======================================================================================================================
// Structures
//======================================================================================================================

/// Information required for authentication.
pub struct Credentials {
    username: String,
    public_key_path: String,
    private_key_path: String,
}

//======================================================================================================================
// Associated Functions
//======================================================================================================================

impl Credentials {
    pub fn new(username: &str, public_key_path: &str, private_key_path: &str) -> Self {
        Self {
            username: username.to_string(),
            public_key_path: public_key_path.to_string(),
            private_key_path: private_key_path.to_string(),
        }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn public_key_path(&self) -> &str {
        &self.public_key_path
    }

    pub fn private_key_path(&self) -> &str {
        &self.private_key_path
    }
}
