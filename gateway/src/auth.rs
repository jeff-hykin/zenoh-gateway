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
    publish: Vec<String>,
    subscribe: Vec<String>,
    /// a newer lease on overlapping keys ends this one instead of being refused
    takeover: bool,
    /// heartbeat silence that ends it; None: the connection's heartbeat deadline
    timeout: Option<Duration>,
    expires: Option<Instant>,
    serial: u64,
}

/// What a client asks to lease (`lease` op).
#[derive(Debug, Clone, Default)]
pub(crate) struct LeaseRequest {
    /// keys only the holder may publish on (a server-defined group's own keys win)
    pub publish: Option<Vec<String>>,
    /// keys only the holder may subscribe to
    pub subscribe: Vec<String>,
    pub takeover: bool,
    pub max_secs: Option<f64>,
    /// heartbeat silence that ends it (`timeoutMs`); None: the connection's heartbeat deadline
    pub timeout: Option<Duration>,
}

/// A lease taken.
#[derive(Debug)]
pub(crate) struct Taken {
    /// its name: the one asked for, else `#<serial>`
    pub group: String,
    pub publish: Vec<String>,
    pub subscribe: Vec<String>,
    pub expires_in: Option<Duration>,
    pub serial: u64,
    /// (group, holder) of the takeover leases this one ended
    pub taken_over: Vec<(String, u64)>,
}

/// The gateway's leases, shared by every frontend.
#[derive(Default)]
pub(crate) struct Leases {
    /// groups the server defines (builder or auth file): name -> keys
    groups: HashMap<String, Vec<String>>,
    held: Mutex<HashMap<String, Held>>,
    serials: AtomicU64,
    /// the longest heartbeat deadline (misses / hz) a lease holder may have (`ServerBuilder::slowest_lease_heartbeat_allowed`)
    pub slowest_heartbeat: Duration,
    /// bumped when a lease is taken or ends, and when a subscription opens or closes ([`crate::Server::changes`])
    pub changes: tokio::sync::watch::Sender<u64>,
}

pub(crate) fn overlaps(a: &[String], b: &[String]) -> bool {
    a.iter().any(|a| b.iter().any(|b| keyexpr::new(a).is_ok_and(|a| keyexpr::new(b).is_ok_and(|b| a.intersects(b)))))
}

impl Leases {
    pub fn new(groups: HashMap<String, Vec<String>>, slowest_heartbeat: Duration) -> Self {
        Leases { groups, slowest_heartbeat, ..Default::default() }
    }

    /// Takes (or renews) `group` for `peer`: the server's publish keys for it, else the request's, all within the grant.
    /// Another client's lease on the same group or overlapping keys refuses it, unless that lease is `takeover`: then
    /// it ends (its holder is in `taken_over`). An unnamed lease needs a grant that may lease any group (`*`).
    pub fn take(&self, peer: u64, grant: &Grant, group: Option<&str>, request: LeaseRequest) -> Result<Taken, String> {
        if !grant.lease_groups.iter().any(|allowed| allowed == "*" || Some(allowed.as_str()) == group) {
            return Err(match group {
                Some(group) => format!("not authorized to lease {group:?}"),
                None => "not authorized to lease without a group name".into(),
            });
        }
        let serial = self.serials.fetch_add(1, Ordering::Relaxed);
        let group = group.map_or_else(|| format!("#{serial}"), str::to_owned);
        let group = group.as_str();
        let publish = self.groups.get(group).cloned().or(request.publish).unwrap_or_default();
        let subscribe = request.subscribe;
        if publish.is_empty() && subscribe.is_empty() {
            return Err(format!("no lease group {group:?} on the server; pass publish or subscribe keys to define it"));
        }
        for (action, keys) in [(Action::Publish, &publish), (Action::Subscribe, &subscribe)] {
            for key in keys {
                grant.allows(action, key).map_err(|_| format!("not authorized to lease {key:?}"))?;
            }
        }
        let secs = match (request.max_secs, grant.max_lease_secs) {
            (Some(asked), Some(limit)) => Some(asked.min(limit)),
            (asked, limit) => asked.or(limit),
        };
        let expires_in = secs.map(|secs| Duration::try_from_secs_f64(secs).map_err(|_| format!("maxSeconds must be positive, got {secs}"))).transpose()?;
        let mut held = self.held.lock();
        let now = Instant::now();
        held.retain(|_, lease| lease.expires.is_none_or(|at| at > now));
        let conflicts: Vec<String> = held
            .iter()
            .filter(|(name, lease)| lease.holder != peer && (*name == group || overlaps(&lease.publish, &publish) || overlaps(&lease.subscribe, &subscribe)))
            .map(|(name, _)| name.clone())
            .collect();
        if let Some(name) = conflicts.iter().find(|name| !held[*name].takeover) {
            return Err(format!("lease {group:?} conflicts with {name:?}, held by another client"));
        }
        let taken_over = conflicts.into_iter().filter_map(|name| held.remove(&name).map(|lease| (name, lease.holder))).collect();
        let expires = expires_in.and_then(|after| now.checked_add(after));
        held.insert(group.to_owned(), Held { holder: peer, publish: publish.clone(), subscribe: subscribe.clone(), takeover: request.takeover, timeout: request.timeout, expires, serial });
        // no `changed()` here: the caller announces it once the holder has its reply, or something reacting to the
        // change could end it and send `leaseLost` ahead of the grant (which the client then never matched)
        Ok(Taken { group: group.to_owned(), publish, subscribe, expires_in, serial, taken_over })
    }

    /// Ends `group` if `which(holder, serial)` says so, returning the holder.
    pub fn end(&self, group: &str, which: impl Fn(u64, u64) -> bool) -> Option<u64> {
        let mut held = self.held.lock();
        let holder = held.get(group).filter(|lease| which(lease.holder, lease.serial))?.holder;
        held.remove(group);
        self.changed();
        Some(holder)
    }

    /// Whether `group` is still the lease numbered `serial`.
    pub fn is_held(&self, group: &str, serial: u64) -> bool {
        self.held.lock().get(group).is_some_and(|lease| lease.serial == serial)
    }

    /// Ends every lease `peer` holds, returning their groups.
    pub fn release_all(&self, peer: u64) -> Vec<String> {
        self.release_where(peer, |_| true)
    }

    /// Ends the leases `peer` holds that follow the connection's heartbeat deadline (no `timeout` of their own).
    pub fn release_on_heartbeat_deadline(&self, peer: u64) -> Vec<String> {
        self.release_where(peer, |lease| lease.timeout.is_none())
    }

    fn release_where(&self, peer: u64, which: impl Fn(&Held) -> bool) -> Vec<String> {
        let mut held = self.held.lock();
        let groups: Vec<String> = held.iter().filter(|(_, lease)| lease.holder == peer && which(lease)).map(|(group, _)| group.clone()).collect();
        held.retain(|group, _| !groups.contains(group));
        if !groups.is_empty() {
            self.changed();
        }
        groups
    }

    pub fn changed(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// The live leases: group and publish keys.
    pub fn held(&self) -> Vec<(String, Vec<String>)> {
        let now = Instant::now();
        self.held.lock().iter().filter(|(_, lease)| lease.expires.is_none_or(|at| at > now)).map(|(group, lease)| (group.clone(), lease.publish.clone())).collect()
    }

    /// Why `peer` may not publish on `key` right now: another client's live lease covers it.
    pub fn blocker(&self, peer: u64, key: &str) -> Option<String> {
        self.find_blocker(peer, key, |lease| &lease.publish)
    }

    /// Why `peer` may not subscribe to `key` right now: another client's live lease covers it.
    pub fn subscribe_blocker(&self, peer: u64, key: &str) -> Option<String> {
        self.find_blocker(peer, key, |lease| &lease.subscribe)
    }

    fn find_blocker(&self, peer: u64, key: &str, keys: impl Fn(&Held) -> &Vec<String>) -> Option<String> {
        let held = self.held.lock();
        let now = Instant::now();
        held.iter()
            .find(|(_, lease)| lease.holder != peer && lease.expires.is_none_or(|at| at > now) && overlaps(keys(lease), &[key.to_owned()]))
            .map(|(group, _)| format!("{key:?} is leased by another client (group {group:?})"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(publish: &[&str], groups: &[&str], max: Option<f64>) -> Grant {
        let keys: Vec<String> = publish.iter().map(|key| key.to_string()).collect();
        Grant { subscribe: keys.clone(), publish: keys, lease_groups: groups.iter().map(|group| group.to_string()).collect(), max_lease_secs: max, ..Default::default() }
    }

    fn publish(keys: Option<Vec<String>>, max_secs: Option<f64>) -> LeaseRequest {
        LeaseRequest { publish: keys, max_secs, ..Default::default() }
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
        let leases = Leases::new(HashMap::from([("arm".to_owned(), vec!["robot/arm/**".to_owned()])]), Duration::from_secs(60));
        let writer = grant(&["robot/**"], &["*"], Some(10.0));
        let (keys, expires_in, _) = leases.take(1, &writer, Some("arm"), publish(None, Some(60.0))).map(|taken| (taken.publish, taken.expires_in, taken.serial)).unwrap();
        assert_eq!((keys, expires_in), (vec!["robot/arm/**".to_owned()], Some(Duration::from_secs(10))));
        assert!(leases.blocker(1, "robot/arm/cmd").is_none(), "the holder publishes");
        assert_eq!(leases.blocker(2, "robot/arm/cmd").unwrap(), "\"robot/arm/cmd\" is leased by another client (group \"arm\")");
        assert!(leases.blocker(2, "robot/base/cmd").is_none());
        // a second client can't take it, nor an ad-hoc group overlapping it
        assert!(leases.take(2, &writer, Some("arm"), publish(None, None)).unwrap_err().contains("held by another client"));
        assert!(leases.take(2, &writer, Some("mine"), publish(Some(vec!["robot/*/cmd".into()]), None)).is_err());
        assert!(leases.take(2, &writer, Some("base"), publish(Some(vec!["robot/base/**".into()]), None)).is_ok());
        // only the holder releases; then the other client may take it
        assert_eq!(leases.end("arm", |holder, _| holder == 2), None);
        assert_eq!(leases.end("arm", |holder, _| holder == 1), Some(1));
        assert!(leases.take(2, &writer, Some("arm"), publish(None, None)).is_ok());
        let mut released = leases.release_all(2);
        released.sort();
        assert_eq!(released, ["arm", "base"]);
        assert!(leases.blocker(1, "robot/arm/cmd").is_none());
    }

    #[test]
    fn leases_respect_the_grant() {
        let leases = Leases::default();
        assert!(leases.take(1, &grant(&["robot/**"], &["arm"], None), Some("base"), publish(Some(vec!["robot/base".into()]), None)).unwrap_err().contains("not authorized to lease"));
        assert!(leases.take(1, &grant(&["robot/arm/**"], &["*"], None), Some("x"), publish(Some(vec!["robot/**".into()]), None)).unwrap_err().contains("not authorized to lease \"robot/**\""));
        assert!(leases.take(1, &grant(&["**"], &["*"], None), Some("x"), publish(None, None)).unwrap_err().contains("pass publish or subscribe keys"));
        assert!(leases.take(1, &grant(&["**"], &["*"], None), Some("x"), publish(Some(vec!["a".into()]), Some(-1.0))).is_err());
    }

    #[test]
    fn a_takeover_lease_goes_to_the_newest_and_a_hard_one_refuses() {
        let leases = Leases::default();
        let everything = grant(&["**"], &["*"], None);
        let seat = |takeover| LeaseRequest { publish: Some(vec!["robot/cmd_vel".into()]), subscribe: vec!["robot/cam/**".into()], takeover, max_secs: None, timeout: None };
        let first = leases.take(1, &everything, Some("robot_control"), seat(true)).unwrap();
        assert!(first.taken_over.is_empty());
        assert!(leases.subscribe_blocker(2, "robot/cam/front").is_some());
        assert!(leases.subscribe_blocker(1, "robot/cam/front").is_none(), "the holder subscribes");
        assert!(leases.subscribe_blocker(2, "robot/tf").is_none());
        assert!(leases.blocker(2, "robot/cam/front").is_none(), "subscribe keys don't block publishing");
        // overlapping only on subscribe keys, under another name: still taken over
        let second = leases.take(2, &everything, Some("viewer"), LeaseRequest { subscribe: vec!["robot/cam/front".into()], ..Default::default() }).unwrap();
        assert_eq!(second.taken_over, [("robot_control".to_owned(), 1)]);
        assert!(leases.subscribe_blocker(1, "robot/cam/front").is_some());
        assert!(leases.blocker(1, "robot/cmd_vel").is_none(), "the lost lease's publish keys are free");
        // `viewer` isn't takeover: a third client is refused
        assert!(leases.take(3, &everything, Some("robot_control"), seat(true)).unwrap_err().contains("held by another client"));
    }

    #[test]
    fn leases_expire() {
        let leases = Leases::default();
        let everything = grant(&["**"], &["*"], None);
        let first = leases.take(1, &everything, Some("g"), publish(Some(vec!["a/b".into()]), Some(0.05))).unwrap().serial;
        assert!(leases.blocker(2, "a/b").is_some());
        std::thread::sleep(Duration::from_millis(80));
        assert!(leases.blocker(2, "a/b").is_none(), "past maxSeconds it blocks no one");
        // a renewal gets a new serial, so the old timer doesn't end it
        let renewed = leases.take(1, &everything, Some("g"), publish(Some(vec!["a/b".into()]), None)).unwrap().serial;
        assert_eq!(leases.end("g", |_, serial| serial == first), None);
        assert_eq!(leases.end("g", |_, serial| serial == renewed), Some(1));
    }
}
