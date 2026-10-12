//! Authorization (a host's hook turns a token into a [`Grant`]) and leases (one client's exclusive
//! right to publish on a group of key expressions, among this gateway's clients).

use serde::Deserialize;
use std::collections::HashMap;
use parking_lot::Mutex;
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
    /// session `delete`s, when set (else `publish` covers them)
    pub delete: Option<Vec<String>>,
    /// `get` queries, queriers, and matching status/listeners for queryables
    pub query: Vec<String>,
    /// queryables it may declare (the browser answers queries)
    pub queryable: Vec<String>,
    /// liveliness: tokens it may declare, and keys it may watch or `get`
    pub liveliness: Vec<String>,
    /// liveliness tokens it may declare, when set (else `liveliness` covers them): keeps watching apart from declaring
    pub liveliness_tokens: Option<Vec<String>>,
    /// which keys `listTopics` shows
    pub list_topics: Vec<String>,
    /// lease groups it may take (`"*"`: any); a lease's keys must also be within `publish`
    pub lease_groups: Vec<String>,
    /// longest lease it may hold, seconds (`None`: no limit)
    pub max_lease_secs: Option<f64>,
    /// may end another client's lease
    pub force_expire: bool,
    /// who the connection is, in the host's words (e.g. an app's name): passed back with each refusal
    /// ([`crate::ServerBuilder::on_refused`])
    pub subject: Option<String>,
    /// zenoh access-control rules for this connection, in place of the lists above when set ([`Acl`])
    pub acl: Option<Acl>,
}

/// zenoh's access-control model ([zenoh docs](https://zenoh.io/docs/manual/access-control/)) for one connection, with
/// the gateway in the router's place: **ingress** is what the browser sends (its puts, declarations, queries, replies),
/// **egress** what the gateway hands it (samples, replies, queries for its queryables, liveliness tokens). A message is
/// decided like zenoh does: denied when a `deny` rule for its message and flow includes its key, else allowed when
/// `default_permission` is `allow` or an `allow` rule includes it, else denied. Refused requests fail with
/// `not authorized to ...`; egress the rules deny is dropped silently, as zenoh drops it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Acl {
    /// what a message no rule decides gets
    pub default_permission: Permission,
    /// the rules, in any order (a deny always wins)
    #[serde(default)]
    pub rules: Vec<AclRule>,
}

/// One rule, zenoh's shape: `flows` left out means both.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AclRule {
    /// a name for the rule
    #[serde(default)]
    pub id: Option<String>,
    /// the messages it decides
    pub messages: Vec<AclMessage>,
    /// the flows it decides (None: both)
    #[serde(default)]
    pub flows: Option<Vec<Flow>>,
    /// the keys it decides: a message's key must be included by one of them
    pub key_exprs: Vec<String>,
    /// allow or deny
    pub permission: Permission,
}

/// A rule's (or the default's) verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// let it through
    Allow,
    /// refuse (ingress) or drop (egress) it
    Deny,
}

/// Which way a message goes, from the gateway's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Flow {
    /// from the browser
    Ingress,
    /// to the browser
    Egress,
}

/// zenoh's ACL messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AclMessage {
    /// a put (ingress: the browser's; egress: a sample it receives)
    Put,
    /// a delete (ingress: the browser's; egress: a delete sample it receives)
    Delete,
    /// declaring a subscriber
    DeclareSubscriber,
    /// a query (ingress: the browser's `get`; egress: a query for its queryable)
    Query,
    /// declaring a queryable
    DeclareQueryable,
    /// a reply (ingress: the browser answering; egress: a reply to its `get`)
    Reply,
    /// a liveliness token (ingress: declaring one; egress: one it watches or gets)
    LivelinessToken,
    /// watching liveliness
    DeclareLivelinessSubscriber,
    /// `livelinessGet`
    LivelinessQuery,
}

impl Acl {
    /// zenoh's decision for `message` on `flow` with `key`: a deny rule including it wins, then the default, then the
    /// allow rules.
    pub fn allows(&self, message: AclMessage, flow: Flow, key: &str) -> bool {
        let Ok(key) = keyexpr::new(key) else { return false };
        let matches = |permission: Permission| {
            self.rules.iter().filter(|rule| rule.permission == permission && rule.messages.contains(&message) && rule.flows.as_ref().is_none_or(|flows| flows.contains(&flow))).any(|rule| rule.key_exprs.iter().any(|rule_key| keyexpr::new(rule_key).is_ok_and(|rule_key| rule_key.includes(key))))
        };
        if matches(Permission::Deny) {
            return false;
        }
        self.default_permission == Permission::Allow || matches(Permission::Allow)
    }
}

/// What a request asks to do; with an [`Acl`] it is the zenoh message the browser sends (ingress).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    Subscribe,
    Publish,
    Delete,
    Query,
    Queryable,
    LivelinessToken,
    LivelinessWatch,
    LivelinessGet,
    List,
}

impl Action {
    fn words(self) -> &'static str {
        match self {
            Action::Subscribe => "subscribe",
            Action::Publish => "publish",
            Action::Delete => "delete",
            Action::Query => "query",
            Action::Queryable => "declare a queryable on",
            Action::LivelinessToken => "declare a liveliness token on",
            Action::LivelinessWatch => "watch liveliness of",
            Action::LivelinessGet => "query liveliness of",
            Action::List => "list",
        }
    }
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
            ..Default::default()
        }
    }

    /// The rules session `delete`s are checked against.
    pub fn deletes(&self) -> &[String] {
        self.delete.as_deref().unwrap_or(&self.publish)
    }

    /// The rules declaring a liveliness token is checked against.
    pub fn tokens(&self) -> &[String] {
        self.liveliness_tokens.as_deref().unwrap_or(&self.liveliness)
    }

    /// Whether this grant allows `action` on `key`: its [`Acl`] when it has one, else the action's list.
    pub(crate) fn allows(&self, action: Action, key: &str) -> Result<(), String> {
        let Some(acl) = &self.acl else {
            let rules: &[String] = match action {
                Action::Subscribe => &self.subscribe,
                Action::Publish => &self.publish,
                Action::Delete => self.deletes(),
                Action::Query => &self.query,
                Action::Queryable => &self.queryable,
                Action::LivelinessToken => self.tokens(),
                Action::LivelinessWatch | Action::LivelinessGet => &self.liveliness,
                Action::List => &self.list_topics,
            };
            return Grant::check(rules, action.words(), key);
        };
        let ingress = |message| acl.allows(message, Flow::Ingress, key);
        let allowed = match action {
            Action::Subscribe => ingress(AclMessage::DeclareSubscriber),
            Action::Publish => ingress(AclMessage::Put),
            Action::Delete => ingress(AclMessage::Delete),
            Action::Query => ingress(AclMessage::Query),
            Action::Queryable => ingress(AclMessage::DeclareQueryable),
            Action::LivelinessToken => ingress(AclMessage::LivelinessToken),
            Action::LivelinessWatch => ingress(AclMessage::DeclareLivelinessSubscriber),
            Action::LivelinessGet => ingress(AclMessage::LivelinessQuery),
            // what it could subscribe to or query
            Action::List => ingress(AclMessage::DeclareSubscriber) || ingress(AclMessage::Query),
        };
        if allowed { Ok(()) } else { Err(format!("not authorized to {} {key:?}", action.words())) }
    }

    /// Whether `message` with `key` may reach the browser (egress); always without an [`Acl`].
    pub fn passes(&self, message: AclMessage, key: &str) -> bool {
        self.acl.as_ref().is_none_or(|acl| acl.allows(message, Flow::Egress, key))
    }

    /// `Err("not authorized to <action> <key>")` unless a rule includes `key`.
    pub fn check(rules: &[String], action: &str, key: &str) -> Result<(), String> {
        let allowed = keyexpr::new(key).is_ok_and(|key| rules.iter().any(|rule| keyexpr::new(rule).is_ok_and(|rule| rule.includes(key))));
        if allowed { Ok(()) } else { Err(format!("not authorized to {action} {key:?}")) }
    }
}

/// The refusal hook: the connection's grant and the reason (`not authorized to <action> "<key>"`), for each request
/// a grant refused ([`crate::ServerBuilder::on_refused`]).
pub type OnRefused = dyn Fn(&Grant, &str) + Send + Sync;

#[derive(Clone)]
pub(crate) struct RefusalHook(pub std::sync::Arc<OnRefused>);

impl std::fmt::Debug for RefusalHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RefusalHook")
    }
}

/// A connection's grant and who hears about its refusals.
pub(crate) struct Access<'a> {
    pub grant: &'a Grant,
    pub on_refused: Option<&'a RefusalHook>,
}

impl Access<'_> {
    /// [`Grant::allows`], telling the refusal hook when it refuses.
    pub fn check(&self, action: Action, key: &str) -> Result<(), String> {
        self.grant.allows(action, key).inspect_err(|reason| {
            if let Some(hook) = self.on_refused {
                (hook.0)(self.grant, reason);
            }
        })
    }

    /// The browser's reply to a query (reply, ingress): with an [`Acl`], allowed like zenoh would; otherwise its
    /// queryable's grant covered it already.
    pub fn reply(&self, key: &str) -> Result<(), String> {
        let Some(acl) = &self.grant.acl else { return Ok(()) };
        if acl.allows(AclMessage::Reply, Flow::Ingress, key) {
            return Ok(());
        }
        let reason = format!("not authorized to reply on {key:?}");
        if let Some(hook) = self.on_refused {
            (hook.0)(self.grant, &reason);
        }
        Err(reason)
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
            grant.allows(Action::Publish, key).map_err(|_| format!("not authorized to lease {key:?}"))?;
        }
        let secs = match (max_secs, grant.max_lease_secs) {
            (Some(asked), Some(limit)) => Some(asked.min(limit)),
            (asked, limit) => asked.or(limit),
        };
        let expires_in = secs.map(|secs| Duration::try_from_secs_f64(secs).map_err(|_| format!("maxSeconds must be positive, got {secs}"))).transpose()?;
        let mut held = self.held.lock();
        let now = Instant::now();
        held.retain(|_, lease| lease.expires.is_none_or(|at| at > now));
        if let Some((name, _)) = held.iter().find(|(name, lease)| lease.holder != peer && (*name == group || overlaps(&lease.keys, &keys))) {
            return Err(format!("lease {group:?} conflicts with {name:?}, held by another client"));
        }
        let serial = self.serials.fetch_add(1, Ordering::Relaxed);
        held.insert(group.to_owned(), Held { holder: peer, keys: keys.clone(), expires: expires_in.and_then(|after| now.checked_add(after)), serial });
        self.changed();
        Ok((keys, expires_in, serial))
    }

    /// Ends `group` if `which(holder, serial)` says so, returning the holder.
    pub fn end(&self, group: &str, which: impl Fn(u64, u64) -> bool) -> Option<u64> {
        let mut held = self.held.lock();
        let holder = held.get(group).filter(|lease| which(lease.holder, lease.serial))?.holder;
        held.remove(group);
        self.changed();
        Some(holder)
    }

    /// Ends every lease `peer` holds, returning their groups.
    pub fn release_all(&self, peer: u64) -> Vec<String> {
        let mut held = self.held.lock();
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
        self.held.lock().iter().filter(|(_, lease)| lease.expires.is_none_or(|at| at > now)).map(|(group, lease)| (group.clone(), lease.keys.clone())).collect()
    }

    /// Why `peer` may not publish on `key` right now: another client's live lease covers it.
    pub fn blocker(&self, peer: u64, key: &str) -> Option<String> {
        let held = self.held.lock();
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
