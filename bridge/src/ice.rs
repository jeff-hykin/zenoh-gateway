//! STUN/TURN servers for both ends of each connection, with coturn's time-limited credentials.

use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A STUN or TURN server, in the browser's `RTCIceServer` shape (`GET /zenoh-web/ice` returns these).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IceServer {
    /// e.g. `stun:stun.example.org:3478`, `turn:relay.example.org:3478?transport=udp`
    pub urls: Vec<String>,
    /// TURN user (left empty with a TURN secret: minted per connection)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    /// TURN password
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub credential: String,
}

/// coturn's TURN REST API credentials (`use-auth-secret`): username `"<unix expiry>:<user>"`, credential
/// base64(HMAC-SHA1(secret, username)), valid for `ttl`.
pub fn turn_credentials(secret: &str, user: &str, ttl: Duration, now: SystemTime) -> (String, String) {
    let expiry = (now + ttl).duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let username = format!("{expiry}:{user}");
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC takes any key length");
    mac.update(username.as_bytes());
    (username, base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

/// The servers with credentials minted for `user` on every TURN entry that has none, when a secret is set.
pub(crate) fn mint(servers: &[IceServer], secret: Option<&(String, Duration)>, user: &str) -> Vec<IceServer> {
    servers
        .iter()
        .map(|server| match secret {
            Some((secret, ttl)) if server.username.is_empty() && server.urls.iter().any(|url| url.starts_with("turn")) => {
                let (username, credential) = turn_credentials(secret, user, *ttl, SystemTime::now());
                IceServer { username, credential, ..server.clone() }
            }
            _ => server.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_match_coturn() {
        // `printf 1700000000:alice | openssl dgst -sha1 -hmac north -binary | base64`, what coturn checks with static-auth-secret=north
        let (username, credential) = turn_credentials("north", "alice", Duration::from_secs(3600), UNIX_EPOCH + Duration::from_secs(1_699_996_400));
        assert_eq!(username, "1700000000:alice");
        assert_eq!(credential, "Cd/49soE35ICqcJF/bCTn8Z4OyE=");
    }

    #[test]
    fn mints_only_turn_entries_without_credentials() {
        let servers = vec![
            IceServer { urls: vec!["stun:s:3478".into()], ..Default::default() },
            IceServer { urls: vec!["turn:t:3478".into()], ..Default::default() },
            IceServer { urls: vec!["turn:u:3478".into()], username: "fixed".into(), credential: "pw".into() },
        ];
        let minted = mint(&servers, Some(&("north".into(), Duration::from_secs(60))), "peer");
        assert_eq!((&minted[0], &minted[2]), (&servers[0], &servers[2]));
        assert!(minted[1].username.ends_with(":peer") && !minted[1].credential.is_empty());
        assert_eq!(mint(&servers, None, "peer"), servers);
    }
}
