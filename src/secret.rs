//! A tiny redacting wrapper so secrets (DB URL, node auth, admin key) never leak
//! into `Debug` output or tracing spans. Mirrors smirk-backend-core's `Secret`.

#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Secret(value.into())
    }

    /// Reveal the inner value. Call sites should keep the exposed value local and
    /// never log it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_is_redacted() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), "Secret(***)");
        assert!(!format!("{s:?}").contains("hunter2"));
        assert_eq!(s.expose(), "hunter2");
    }
}
