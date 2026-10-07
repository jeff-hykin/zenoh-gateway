//! Authorization (a host's hook turns a token into a [`Grant`]) and leases (one client's exclusive
//! right to publish on a group of key expressions, among this gateway's clients).

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use zenoh::key_expr::keyexpr;

/// What one connection may do. Each list holds key expressions; a request is allowed when one of
/// them includes its key. [`Grant::all`] is what a server without an authorize hook gives everyone.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, default)]
pub struct Grant {
    /// subscriptions
    pub subscribe: Vec<String>,
    /// publishers (and their deadmen), session `put`/`delete`, and matching status/listeners for subscribers
    pub publish: Vec<String>,
    /// `get` queries, queriers, and matching status/listeners for queryables
    pub query: Vec<String>,
    /// queryables it may declare (the browser answers queries)
    pub queryable: Vec<String>,
    /// liveliness: tokens it may declare, and keys it may watch or `get`
    pub liveliness: Vec<String>,
    /// which keys `listTopics` shows
    pub list_topics: Vec<String>,
    /// lease groups it may take (`"*"`: any); a lease's keys must also be within `publish`
    pub lease_groups: Vec<String>,
    /// longest lease it may hold, seconds (`None`: no limit)
    pub max_lease_secs: Option<f64>,
    /// may end another client's lease
    pub force_expire: bool,
}

impl Grant {
    /// Everything, including force-expiring leases.
    pub fn all() -> Self {
        let any = || vec!["**".to_owned()];
        Grant {
            subscribe: any(),
            publish: any(),
            query: any(),
            queryable: any(),
            liveliness: any(),
            list_topics: any(),
            lease_groups: vec!["*".into()],
            max_lease_secs: None,
            force_expire: true,
        }
    }

    /// `Err("not authorized to <action> <key>")` unless a rule includes `key`.
    pub fn check(rules: &[String], action: &str, key: &str) -> Result<(), String> {
        let allowed = keyexpr::new(key).is_ok_and(|key| rules.iter().any(|rule| keyexpr::new(rule).is_ok_and(|rule| rule.includes(key))));
        if allowed { Ok(()) } else { Err(format!("not authorized to {action} {key:?}")) }
    }
}

struct Held {
    holder: u64,
    keys: Vec<String>,
    expires: Option<Instant>,
    serial: u64,
}

/// The gateway's leases, shared by every frontend.
#[derive(Default)]
pub(crate) struct Leases {
    /// groups the server defines (builder or auth file): name -> keys
    groups: HashMap<String, Vec<String>>,
    held: Mutex<HashMap<String, Held>>,
    serials: AtomicU64,
    /// bumped when a lease is taken or ends, and when a subscription opens or closes ([`crate::Server::changes`])
    pub changes: tokio::sync::watch::Sender<u64>,
}

fn overlaps(a: &[String], b: &[String]) -> bool {
    a.iter().any(|a| b.iter().any(|b| keyexpr::new(a).is_ok_and(|a| keyexpr::new(b).is_ok_and(|b| a.intersects(b)))))
}

impl Leases {
    pub fn new(groups: HashMap<String, Vec<String>>) -> Self {
        Leases { groups, ..Default::default() }
    }

    /// Takes (or renews) `group` for `peer`: the server's keys for it, else `keys`, all within the grant.
    /// Returns the keys, when it ends at the latest, and its serial number.
    pub fn take(&self, peer: u64, grant: &Grant, group: &str, keys: Option<Vec<String>>, max_secs: Option<f64>) -> Result<(Vec<String>, Option<Duration>, u64), String> {
        if !grant.lease_groups.iter().any(|allowed| allowed == "*" || allowed == group) {
            return Err(format!("not authorized to lease {group:?}"));
        }
        let keys = match self.groups.get(group) {
            Some(keys) => keys.clone(),
            None => keys.filter(|keys| !keys.is_empty()).ok_or_else(|| format!("no lease group {group:?} on the server; pass keys to define it"))?,
        };
        for key in &keys {
            Grant::check(&grant.publish, "lease", key)?;
        }
        let secs = match (max_secs, grant.max_lease_secs) {
            (Some(asked), Some(limit)) => Some(asked.min(limit)),
            (asked, limit) => asked.or(limit),
        };
        let expires_in = secs.map(|secs| Duration::try_from_secs_f64(secs).map_err(|_| format!("maxSeconds must be positive, got {secs}"))).transpose()?;
        let mut held = self.held.lock().unwrap();
        let now = Instant::now();
        held.retain(|_, lease| lease.expires.is_none_or(|at| at > now));
        if let Some((name, _)) = held.iter().find(|(name, lease)| lease.holder != peer && (*name == group || overlaps(&lease.keys, &keys))) {
            return Err(format!("lease {group:?} conflicts with {name:?}, held by another client"));
        }
        let serial = self.serials.fetch_add(1, Ordering::Relaxed);
        held.insert(group.to_owned(), Held { holder: peer, keys: keys.clone(), expires: expires_in.map(|after| now + after), serial });
        self.changed();
        Ok((keys, expires_in, serial))
    }

    /// Ends `group` if `which(holder, serial)` says so, returning the holder.
    pub fn end(&self, group: &str, which: impl Fn(u64, u64) -> bool) -> Option<u64> {
        let mut held = self.held.lock().unwrap();
        let holder = held.get(group).filter(|lease| which(lease.holder, lease.serial))?.holder;
        held.remove(group);
        self.changed();
        Some(holder)
    }

    /// Ends every lease `peer` holds, returning their groups.
    pub fn release_all(&self, peer: u64) -> Vec<String> {
        let mut held = self.held.lock().unwrap();
        let groups: Vec<String> = held.iter().filter(|(_, lease)| lease.holder == peer).map(|(group, _)| group.clone()).collect();
        held.retain(|_, lease| lease.holder != peer);
        if !groups.is_empty() {
            self.changed();
        }
        groups
    }

    pub fn changed(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// The live leases: group and keys.
    pub fn held(&self) -> Vec<(String, Vec<String>)> {
        let now = Instant::now();
        self.held.lock().unwrap().iter().filter(|(_, lease)| lease.expires.is_none_or(|at| at > now)).map(|(group, lease)| (group.clone(), lease.keys.clone())).collect()
    }

    /// Why `peer` may not publish on `key` right now: another client's live lease covers it.
    pub fn blocker(&self, peer: u64, key: &str) -> Option<String> {
        let held = self.held.lock().unwrap();
        let now = Instant::now();
        held.iter()
            .find(|(_, lease)| lease.holder != peer && lease.expires.is_none_or(|at| at > now) && overlaps(&lease.keys, &[key.to_owned()]))
            .map(|(group, _)| format!("{key:?} is leased by another client (group {group:?})"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(publish: &[&str], groups: &[&str], max: Option<f64>) -> Grant {
        Grant { publish: publish.iter().map(|key| key.to_string()).collect(), lease_groups: groups.iter().map(|group| group.to_string()).collect(), max_lease_secs: max, ..Default::default() }
    }

    #[test]
    fn rules_include_keys() {
        let rules = vec!["robot/*/state".to_owned(), "camera/**".to_owned()];
        assert!(Grant::check(&rules, "subscribe", "robot/arm/state").is_ok());
        assert!(Grant::check(&rules, "subscribe", "camera/front/image").is_ok());
        assert!(Grant::check(&rules, "subscribe", "camera/**").is_ok());
        assert_eq!(Grant::check(&rules, "subscribe", "robot/**").unwrap_err(), "not authorized to subscribe \"robot/**\"");
        assert!(Grant::check(&rules, "subscribe", "robot/arm/cmd").is_err());
        assert!(Grant::check(&[], "publish", "a").is_err());
        assert!(Grant::check(&rules, "subscribe", "bad//key").is_err());
        assert!(Grant::check(&Grant::all().publish, "publish", "any/key/at/all").is_ok());
    }

    #[test]
    fn grant_parses_with_defaults() {
        let parsed: Grant = serde_json::from_value(serde_json::json!({"subscribe": ["**"], "maxLeaseSecs": 30})).unwrap();
        assert_eq!(parsed, Grant { subscribe: vec!["**".into()], max_lease_secs: Some(30.0), ..Default::default() });
        assert!(serde_json::from_value::<Grant>(serde_json::json!({"subscibe": ["**"]})).is_err());
    }

    #[test]
    fn leases_exclude_other_clients() {
        let leases = Leases::new(HashMap::from([("arm".to_owned(), vec!["robot/arm/**".to_owned()])]));
        let writer = grant(&["robot/**"], &["*"], Some(10.0));
        let (keys, expires_in, _) = leases.take(1, &writer, "arm", None, Some(60.0)).unwrap();
        assert_eq!((keys, expires_in), (vec!["robot/arm/**".to_owned()], Some(Duration::from_secs(10))));
        assert!(leases.blocker(1, "robot/arm/cmd").is_none(), "the holder publishes");
        assert_eq!(leases.blocker(2, "robot/arm/cmd").unwrap(), "\"robot/arm/cmd\" is leased by another client (group \"arm\")");
        assert!(leases.blocker(2, "robot/base/cmd").is_none());
        // a second client can't take it, nor an ad-hoc group overlapping it
        assert!(leases.take(2, &writer, "arm", None, None).unwrap_err().contains("held by another client"));
        assert!(leases.take(2, &writer, "mine", Some(vec!["robot/*/cmd".into()]), None).is_err());
        assert!(leases.take(2, &writer, "base", Some(vec!["robot/base/**".into()]), None).is_ok());
        // only the holder releases; then the other client may take it
        assert_eq!(leases.end("arm", |holder, _| holder == 2), None);
        assert_eq!(leases.end("arm", |holder, _| holder == 1), Some(1));
        assert!(leases.take(2, &writer, "arm", None, None).is_ok());
        let mut released = leases.release_all(2);
        released.sort();
        assert_eq!(released, ["arm", "base"]);
        assert!(leases.blocker(1, "robot/arm/cmd").is_none());
    }

    #[test]
    fn leases_respect_the_grant() {
        let leases = Leases::default();
        assert!(leases.take(1, &grant(&["robot/**"], &["arm"], None), "base", Some(vec!["robot/base".into()]), None).unwrap_err().contains("not authorized to lease"));
        assert!(leases.take(1, &grant(&["robot/arm/**"], &["*"], None), "x", Some(vec!["robot/**".into()]), None).unwrap_err().contains("not authorized to lease \"robot/**\""));
        assert!(leases.take(1, &grant(&["**"], &["*"], None), "x", None, None).unwrap_err().contains("pass keys"));
        assert!(leases.take(1, &grant(&["**"], &["*"], None), "x", Some(vec!["a".into()]), Some(-1.0)).is_err());
    }

    #[test]
    fn leases_expire() {
        let leases = Leases::default();
        let everything = grant(&["**"], &["*"], None);
        let (_, _, first) = leases.take(1, &everything, "g", Some(vec!["a/b".into()]), Some(0.05)).unwrap();
        assert!(leases.blocker(2, "a/b").is_some());
        std::thread::sleep(Duration::from_millis(80));
        assert!(leases.blocker(2, "a/b").is_none(), "past maxSeconds it blocks no one");
        // a renewal gets a new serial, so the old timer doesn't end it
        let (_, _, renewed) = leases.take(1, &everything, "g", Some(vec!["a/b".into()]), None).unwrap();
        assert_eq!(leases.end("g", |_, serial| serial == first), None);
        assert_eq!(leases.end("g", |_, serial| serial == renewed), Some(1));
    }
}
