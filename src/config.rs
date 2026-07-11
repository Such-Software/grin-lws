//! Service configuration, loaded entirely from the environment.
//!
//! PUBLIC-CLEAN: every value is an env var with a generic loopback default. No
//! hostnames, no operator IPs, no secrets baked in. Anyone can point this at
//! their own grin node + database and run it. See `.env.example`.

use std::net::SocketAddr;

use crate::secret::Secret;

#[derive(Clone)]
pub struct Config {
    /// Address to bind the HTTP server to (loopback / a private network).
    pub bind_addr: SocketAddr,

    /// Database connection URL. `postgres://…` for scale, or `sqlite://path.db`
    /// for a single-operator deployment.
    pub database_url: Secret,

    /// grin node Foreign API base URL (v2 JSON-RPC): tip, block/output reads,
    /// and `push_transaction` for broadcast.
    pub node_foreign_api_url: String,
    /// Optional basic-auth secret for the node Foreign API (empty = none).
    pub node_foreign_api_secret: Secret,

    /// How often the background scanner polls for new blocks.
    pub scan_poll_secs: u64,
    /// How far behind the tip a newly-registered account may start scanning from
    /// (restore-depth bound). `0` = unbounded.
    pub restore_max_depth_days: u32,

    /// Optional shared bearer that gates the admin API (`/list_accounts`,
    /// `/rescan`). Empty = admin API disabled.
    pub admin_key: Secret,
}

impl Config {
    pub fn from_env() -> Self {
        let env_or = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let parse_or = |k: &str, d: u64| env_or(k, &d.to_string()).parse().unwrap_or(d);

        let bind_addr = env_or("GRINLWS_BIND", "127.0.0.1:3480")
            .parse()
            .expect("GRINLWS_BIND must be a valid socket address");

        Config {
            bind_addr,
            database_url: Secret::new(env_or("DATABASE_URL", "sqlite://grin-lws.db")),
            node_foreign_api_url: env_or(
                "GRIN_NODE_URL",
                "http://127.0.0.1:3413/v2/foreign",
            ),
            node_foreign_api_secret: Secret::new(env_or("GRIN_NODE_API_SECRET", "")),
            scan_poll_secs: parse_or("GRINLWS_SCAN_POLL_SECS", 15),
            restore_max_depth_days: parse_or("GRINLWS_RESTORE_MAX_DEPTH_DAYS", 0) as u32,
            admin_key: Secret::new(env_or("GRINLWS_ADMIN_KEY", "")),
        }
    }

    /// Whether the admin API is enabled (an admin key was configured).
    pub fn admin_enabled(&self) -> bool {
        !self.admin_key.expose().is_empty()
    }
}
