//! Bridge-side enforcement of the zenoh config's `access_control` section.
//!
//! zenoh's own ACL also filters what the bridge's session sends (egress interceptors), but it
//! drops silently. This applies the same rules before a browser's put / subscribe / get reaches
//! zenoh, so the browser gets a reason. Decision logic mirrors zenoh's policy decision point:
//! a matching deny rule wins; otherwise `default_permission`, unless it is deny and an allow rule
//! matches. A rule matches when one of its key expressions includes the requested key.
//! Browsers have no zenoh subject (interface, username, certificate), so every rule referenced
//! by any policy applies to them, whatever its subjects.

use serde::Deserialize;
use zenoh::key_expr::OwnedKeyExpr;

/// The zenoh ACL message names the bridge checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclMessage {
    Put,
    DeclareSubscriber,
    Query,
}

impl AclMessage {
    fn name(self) -> &'static str {
        match self {
            AclMessage::Put => "put",
            AclMessage::DeclareSubscriber => "declare_subscriber",
            AclMessage::Query => "query",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Permission {
    Allow,
    Deny,
}

#[derive(Debug, Deserialize)]
struct RuleConfig {
    id: String,
    messages: Vec<String>,
    #[serde(default)]
    flows: Option<Vec<String>>,
    permission: Permission,
    key_exprs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PolicyConfig {
    rules: Vec<String>,
}

/// zenoh reports unset fields as `null`, so every field is optional.
#[derive(Debug, Deserialize)]
struct AclConfig {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    default_permission: Option<Permission>,
    #[serde(default)]
    rules: Option<Vec<RuleConfig>>,
    #[serde(default)]
    policies: Option<Vec<PolicyConfig>>,
}

#[derive(Debug)]
struct Rule {
    id: String,
    messages: Vec<String>,
    permission: Permission,
    key_exprs: Vec<OwnedKeyExpr>,
}

/// Rules that apply to messages leaving the bridge into zenoh (the `egress` flow).
#[derive(Debug, Default)]
pub struct AccessControl {
    enabled: bool,
    default_allow: bool,
    rules: Vec<Rule>,
}

impl AccessControl {
    /// Reads `access_control` from a zenoh config; absent or disabled means allow everything.
    pub fn from_config(config: &zenoh::Config) -> anyhow::Result<Self> {
        let Ok(json) = config.get_json("access_control") else { return Ok(Self::default()) };
        Self::from_json(&json)
    }

    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        let parsed: Option<AclConfig> = serde_json::from_str(json)?;
        let Some(parsed) = parsed.filter(|acl| acl.enabled == Some(true)) else { return Ok(Self::default()) };
        let policies = parsed.policies.unwrap_or_default();
        let referenced: Vec<&str> = policies.iter().flat_map(|p| p.rules.iter().map(String::as_str)).collect();
        let mut rules = Vec::new();
        for rule in parsed.rules.unwrap_or_default() {
            let egress = rule.flows.as_ref().is_none_or(|flows| flows.iter().any(|f| f == "egress"));
            if !egress || !referenced.contains(&rule.id.as_str()) {
                continue;
            }
            let key_exprs = rule
                .key_exprs
                .iter()
                .map(|ke| OwnedKeyExpr::autocanonize(ke.clone()).map_err(|e| anyhow::anyhow!("rule {}: key_expr {ke:?}: {e}", rule.id)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            rules.push(Rule { id: rule.id, messages: rule.messages, permission: rule.permission, key_exprs });
        }
        // zenoh's default permission is deny
        let default_allow = parsed.default_permission == Some(Permission::Allow);
        Ok(AccessControl { enabled: true, default_allow, rules })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// `Err(reason)` when the browser may not do `message` on `key`.
    pub fn check(&self, message: AclMessage, key: &str) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        let key_expr = OwnedKeyExpr::autocanonize(key.to_owned()).map_err(|e| format!("invalid key expression {key:?}: {e}"))?;
        let matching = |permission: Permission| {
            self.rules.iter().find(|rule| {
                rule.permission == permission
                    && rule.messages.iter().any(|m| m == message.name())
                    && rule.key_exprs.iter().any(|rule_ke| rule_ke.includes(&key_expr))
            })
        };
        if let Some(rule) = matching(Permission::Deny) {
            return Err(format!("access_control rule {:?} denies {} on {key:?}", rule.id, message.name()));
        }
        if self.default_allow || matching(Permission::Allow).is_some() {
            return Ok(());
        }
        Err(format!("access_control default_permission denies {} on {key:?}", message.name()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(json: &str) -> AccessControl {
        AccessControl::from_json(json).unwrap()
    }

    #[test]
    fn zenoh_default_section_parses() {
        // what zenoh::Config::get_json("access_control") returns when the config has no ACL
        let unset = r#"{"enabled":false,"default_permission":"deny","rules":null,"subjects":null,"policies":null}"#;
        assert!(!acl(unset).enabled());
        assert!(!acl("null").enabled());
        let config = zenoh::Config::default();
        assert!(!AccessControl::from_config(&config).unwrap().enabled());
    }

    #[test]
    fn disabled_allows_everything() {
        let control = acl(r#"{"enabled": false, "default_permission": "deny"}"#);
        assert!(control.check(AclMessage::Put, "cmd_vel").is_ok());
    }

    #[test]
    fn deny_rule_wins_over_default_allow() {
        let control = acl(r#"{
            "enabled": true, "default_permission": "allow",
            "rules": [{"id": "no-cmd", "messages": ["put"], "flows": ["egress"], "permission": "deny", "key_exprs": ["cmd_vel"]}],
            "subjects": [{"id": "all"}],
            "policies": [{"rules": ["no-cmd"], "subjects": ["all"]}]
        }"#);
        assert!(control.check(AclMessage::Put, "cmd_vel").unwrap_err().contains("no-cmd"));
        assert!(control.check(AclMessage::Put, "other").is_ok());
        assert!(control.check(AclMessage::DeclareSubscriber, "cmd_vel").is_ok(), "rule only covers put");
    }

    #[test]
    fn default_deny_needs_an_including_allow() {
        let control = acl(r#"{
            "enabled": true, "default_permission": "deny",
            "rules": [
                {"id": "cams", "messages": ["declare_subscriber"], "permission": "allow", "key_exprs": ["camera/**"]},
                {"id": "unused", "messages": ["put"], "permission": "allow", "key_exprs": ["**"]}
            ],
            "policies": [{"rules": ["cams"], "subjects": ["all"]}]
        }"#);
        assert!(control.check(AclMessage::DeclareSubscriber, "camera/front").is_ok());
        assert!(control.check(AclMessage::DeclareSubscriber, "**").is_err(), "camera/** does not include **");
        assert!(control.check(AclMessage::Put, "anything").is_err(), "rules outside every policy have no effect");
    }

    #[test]
    fn ingress_only_rules_do_not_apply() {
        let control = acl(r#"{
            "enabled": true, "default_permission": "allow",
            "rules": [{"id": "in", "messages": ["put"], "flows": ["ingress"], "permission": "deny", "key_exprs": ["**"]}],
            "policies": [{"rules": ["in"], "subjects": ["all"]}]
        }"#);
        assert!(control.check(AclMessage::Put, "a").is_ok());
    }
}
