//! The gateway's running list of topics (SPEC "Topic enumeration"): liveliness tokens as they come and go, and
//! every key a browser's subscription receives, kept for the gateway's lifetime so `listTopics` answers at once.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde_json::{Value, json};
use zenoh::key_expr::KeyExpr;
use zenoh::sample::SampleKind;

#[derive(Default)]
struct Entry {
    /// live liveliness tokens naming this key -> their source (`token` or `advancedPublisher`)
    tokens: BTreeMap<String, &'static str>,
    /// when a subscription last received a sample on this key
    last_sample: Option<Instant>,
}

/// Every topic the gateway has seen, with where it was seen.
pub struct TopicCatalog {
    entries: Mutex<BTreeMap<String, Entry>>,
    /// a key only seen in samples is forgotten after this long without one
    sample_memory: Duration,
    last_prune: Mutex<Instant>,
    /// the liveliness subscriber, kept for the catalog's lifetime
    _liveliness: Mutex<Option<zenoh::pubsub::Subscriber<()>>>,
}

/// The topic a liveliness token names, and its source.
fn token_topic(token: &str) -> (&str, &'static str) {
    match token.split_once("/@adv/pub/") {
        Some((key, _)) => (key, "advancedPublisher"),
        None => (token, "token"),
    }
}

impl TopicCatalog {
    /// Starts following liveliness tokens on `session` (with their history, so tokens declared earlier count too).
    pub async fn start(session: &zenoh::Session, sample_memory: Duration) -> anyhow::Result<Arc<Self>> {
        let catalog = Arc::new(TopicCatalog { entries: Mutex::default(), sample_memory, last_prune: Mutex::new(Instant::now()), _liveliness: Mutex::new(None) });
        let weak = Arc::downgrade(&catalog);
        let subscriber = session
            .liveliness()
            .declare_subscriber("**")
            .history(true)
            .callback(move |sample| {
                if let Some(catalog) = weak.upgrade() {
                    catalog.note_token(sample.key_expr().as_str(), sample.kind() == SampleKind::Put);
                }
            })
            .await
            .map_err(|error| anyhow::anyhow!("following liveliness tokens: {error}"))?;
        *catalog._liveliness.lock() = Some(subscriber);
        Ok(catalog)
    }

    fn note_token(&self, token: &str, alive: bool) {
        let (key, source) = token_topic(token);
        let mut entries = self.entries.lock();
        if alive {
            entries.entry(key.to_owned()).or_default().tokens.insert(token.to_owned(), source);
        } else if let Some(entry) = entries.get_mut(key) {
            entry.tokens.remove(token);
            if entry.tokens.is_empty() && entry.last_sample.is_none() {
                entries.remove(key);
            }
        }
    }

    /// A subscription received a sample on `key`.
    pub fn note_sample(&self, key: &str) {
        let now = Instant::now();
        let mut entries = self.entries.lock();
        match entries.get_mut(key) {
            Some(entry) => entry.last_sample = Some(now),
            None => {
                entries.insert(key.to_owned(), Entry { tokens: BTreeMap::new(), last_sample: Some(now) });
                drop(entries);
                self.prune_if_due(now);
            }
        }
    }

    /// Forgets sample-only keys older than `sample_memory`, at most once per `sample_memory`.
    fn prune_if_due(&self, now: Instant) {
        {
            let mut last = self.last_prune.lock();
            if now.duration_since(*last) < self.sample_memory {
                return;
            }
            *last = now;
        }
        self.prune(now);
    }

    fn prune(&self, now: Instant) {
        let memory = self.sample_memory;
        self.entries.lock().retain(|_, entry| {
            if entry.last_sample.is_some_and(|seen| now.duration_since(seen) > memory) {
                entry.last_sample = None;
            }
            !entry.tokens.is_empty() || entry.last_sample.is_some()
        });
    }

    /// The topics under `filter`: `{key, sources}` like `listTopics` answers.
    pub fn list(&self, filter: &KeyExpr<'_>) -> BTreeMap<String, BTreeSet<&'static str>> {
        self.prune(Instant::now());
        let entries = self.entries.lock();
        entries
            .iter()
            .filter(|(key, _)| KeyExpr::try_from(key.as_str()).is_ok_and(|key| filter.intersects(&key)))
            .map(|(key, entry)| {
                let mut sources: BTreeSet<&'static str> = entry.tokens.values().copied().collect();
                if entry.last_sample.is_some() {
                    sources.insert("sample");
                }
                (key.clone(), sources)
            })
            .collect()
    }
}

/// `found` as `listTopics` answers it.
pub fn to_json(found: BTreeMap<String, BTreeSet<&'static str>>) -> Vec<Value> {
    found.into_iter().map(|(key, sources)| json!({"key": key, "sources": sources})).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(memory: Duration) -> TopicCatalog {
        TopicCatalog { entries: Mutex::default(), sample_memory: memory, last_prune: Mutex::new(Instant::now()), _liveliness: Mutex::new(None) }
    }

    fn listed(catalog: &TopicCatalog, filter: &str) -> Vec<(String, Vec<&'static str>)> {
        catalog.list(&KeyExpr::try_from(filter).unwrap()).into_iter().map(|(key, sources)| (key, sources.into_iter().collect())).collect()
    }

    #[test]
    fn tokens_come_and_go() {
        let catalog = catalog(Duration::from_secs(60));
        catalog.note_token("robot/arm", true);
        catalog.note_token("robot/cam/@adv/pub/abc/1/2", true);
        assert_eq!(listed(&catalog, "robot/**"), vec![("robot/arm".into(), vec!["token"]), ("robot/cam".into(), vec!["advancedPublisher"])]);
        catalog.note_token("robot/arm", false);
        assert_eq!(listed(&catalog, "robot/**"), vec![("robot/cam".into(), vec!["advancedPublisher"])]);
        assert!(listed(&catalog, "other/**").is_empty());
    }

    #[test]
    fn samples_are_forgotten_after_their_memory() {
        let catalog = catalog(Duration::from_millis(50));
        catalog.note_sample("robot/odom");
        catalog.note_token("robot/odom", true);
        catalog.note_sample("robot/map");
        assert_eq!(listed(&catalog, "**"), vec![("robot/map".into(), vec!["sample"]), ("robot/odom".into(), vec!["sample", "token"])]);
        std::thread::sleep(Duration::from_millis(80));
        // the token keeps robot/odom; robot/map was only ever a sample
        assert_eq!(listed(&catalog, "**"), vec![("robot/odom".into(), vec!["token"])]);
    }
}
