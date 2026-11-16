//! Owned synthetic connector APIs: process lifetime, handshake and control.

use super::*;

pub(super) struct Server {
    child: std::process::Child,
    pub endpoint: String,
    pub connection: Value,
    http: reqwest::Client,
}

impl Server {
    pub fn start(script: &str) -> Self {
        let checkout = common::repo();
        let child = Command::new(checkout.join("connectors/.venv/bin/python"))
            .arg(checkout.join("connectors/tests").join(script))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("synthetic connector API; run scripts/doctor");
        // Own the child before parsing so a failed handshake also terminates it.
        let mut server = Self {
            child,
            endpoint: String::new(),
            connection: Value::Null,
            http: reqwest::Client::builder().no_proxy().build().unwrap(),
        };
        let mut line = String::new();
        BufReader::new(server.child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        server.connection = serde_json::from_str(&line).expect("synthetic API connection line");
        server.endpoint = server.connection["endpoint"].as_str().unwrap().into();
        server
    }

    pub fn call(&self, operation: &str, arguments: Value) -> Value {
        let output = Command::new("curl")
            .args([
                "-fsS",
                "--noproxy",
                "*",
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "-d",
            ])
            .arg(arguments.to_string())
            .arg(format!("{}/fake/{operation}", self.endpoint))
            .output()
            .expect("synthetic API control");
        assert!(
            output.status.success(),
            "synthetic API {operation}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let body: Value = serde_json::from_slice(&output.stdout).unwrap();
        body.get("result").expect("synthetic API result").clone()
    }

    pub async fn call_async(&self, operation: &str, arguments: Value) -> Value {
        let response = self
            .http
            .post(format!("{}/fake/{operation}", self.endpoint))
            .json(&arguments)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert!(status.is_success(), "synthetic API {operation}: {body}");
        body.get("result").expect("synthetic API result").clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
