//! state — per-user typed key/value state with optional constraints.
//!
//! Each chat user has a JSON-like bag of properties. **Anyone may read or edit
//! anyone's** properties. A property's value is typed (int / float / string /
//! bool / options) and may be constrained by an optional `type`, `min`/`max`,
//! and `options` (an array of allowed strings).
//!
//!   `!state set [@user] <key>=<value>`   set (add or modify)
//!   `!state get [@user] <key>`           read
//!   `!state list [@user]`                list keys + values
//!   `!state remove [@user] <key>`        delete
//!   `!state define <key> [type=.. min=.. max=.. options=a|b|c]`   constrain a key
//!   `!state help`
//!
//! Constraints come from the operator's `config.json` schema AND from
//! user-defined `!state define` calls (merged; the operator wins). Without a
//! constraint the value's type is inferred from the text.
//!
//! Storage: one `turso` DB per server (`<db_dir>/<guild_id>.db`) + a global DB.

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tracing::{info, warn};
use tracing_subscriber::FmtSubscriber;

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    WsMessage,
>;

const STATE_COMMAND: &str = "state";
const DEFAULT_FLAG: &str = "!";
const DEFAULT_DB_DIR: &str = "~/.cockatiel/state";
const DEFAULT_RECONNECT_BASE_SECS: u32 = 1;
const DEFAULT_RECONNECT_MAX_SECS: u32 = 30;
const DM_DB: &str = "_dm";
/// Hard caps so a single user cannot bloat the store.
const MAX_KEYS_PER_SUBJECT: usize = 200;
const MAX_KEY_LEN: usize = 64;
const MAX_VALUE_LEN: usize = 1024;

// ── config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Settings {
    command_flag: String,
    db_dir: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
    /// The operator's constraints: key -> Constraint.
    schema: HashMap<String, Constraint>,
}

fn ensure_defaults() -> Settings {
    let root: Option<serde_json::Value> = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|d| serde_json::from_str(&d).ok());
    let ms = root
        .as_ref()
        .and_then(|r| r.get("module_specific"))
        .and_then(|m| m.as_object())
        .cloned()
        .unwrap_or_default();
    let command_flag = ms.get("command_flag").and_then(|v| v.as_str()).unwrap_or(DEFAULT_FLAG).to_string();
    let db_dir = ms.get("db_dir").and_then(|v| v.as_str()).unwrap_or(DEFAULT_DB_DIR).to_string();
    let reconnect_base_secs = ms.get("reconnect_base_secs").and_then(|v| v.as_u64()).map(|v| v as u32).unwrap_or(DEFAULT_RECONNECT_BASE_SECS);
    let reconnect_max_secs = ms.get("reconnect_max_secs").and_then(|v| v.as_u64()).map(|v| v as u32).unwrap_or(DEFAULT_RECONNECT_MAX_SECS);
    let schema = ms.get("schema").map(parse_schema).unwrap_or_default();

    if let Some(mut root) = root {
        if let Some(obj) = root.as_object_mut() {
            if let Some(ms) = obj
                .entry("module_specific".to_string())
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
            {
                ms.entry("command_flag".to_string()).or_insert_with(|| serde_json::json!(command_flag));
                ms.entry("db_dir".to_string()).or_insert_with(|| serde_json::json!(db_dir));
                ms.entry("reconnect_base_secs".to_string()).or_insert_with(|| serde_json::json!(reconnect_base_secs));
                ms.entry("reconnect_max_secs".to_string()).or_insert_with(|| serde_json::json!(reconnect_max_secs));
                ms.entry("schema".to_string()).or_insert_with(|| serde_json::json!({}));
            }
            let _ = std::fs::write("config.json", serde_json::to_string_pretty(&root).unwrap());
        }
    }
    Settings { command_flag, db_dir, reconnect_base_secs, reconnect_max_secs, schema }
}

/// Parse the operator schema: `{"key": {"type":"int","min":0,"max":100}}`.
fn parse_schema(v: &serde_json::Value) -> HashMap<String, Constraint> {
    let mut out = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, c) in obj {
            let con = Constraint {
                ctype: c.get("type").and_then(|x| x.as_str()).map(|s| s.to_ascii_lowercase()),
                min: c.get("min").and_then(|x| x.as_f64()),
                max: c.get("max").and_then(|x| x.as_f64()),
                options: c.get("options").and_then(|x| x.as_array()).map(|a| {
                    a.iter().filter_map(|s| s.as_str().map(|s| s.to_string())).collect()
                }),
            };
            out.insert(k.clone(), con);
        }
    }
    out
}

fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    } else if path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }
    path.to_string()
}

// ── the value + constraint model (pure, unit-tested) ────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

impl Value {
    fn type_name(&self) -> &'static str {
        match self {
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Bool(_) => "bool",
            Value::Str(_) => "string",
        }
    }

    /// The stored JSON scalar (strings quoted, numbers/bools bare).
    fn to_json(&self) -> String {
        match self {
            Value::Int(i) => serde_json::Value::from(*i).to_string(),
            Value::Float(f) => serde_json::Value::from(*f).to_string(),
            Value::Bool(b) => serde_json::Value::from(*b).to_string(),
            Value::Str(s) => serde_json::Value::from(s.as_str()).to_string(),
        }
    }

    fn from_json(s: &str) -> Option<Value> {
        match serde_json::from_str::<serde_json::Value>(s).ok()? {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Some(Value::Int(i))
                } else {
                    n.as_f64().map(Value::Float)
                }
            }
            serde_json::Value::Bool(b) => Some(Value::Bool(b)),
            serde_json::Value::String(s) => Some(Value::Str(s)),
            _ => None,
        }
    }

    /// Human-readable, as shown in replies.
    fn display(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => s.clone(),
        }
    }
}

/// An optional constraint on a key's value.
#[derive(Debug, Clone, Default, PartialEq)]
struct Constraint {
    ctype: Option<String>, // int | float | string | bool | options
    min: Option<f64>,
    max: Option<f64>,
    options: Option<Vec<String>>,
}

impl Constraint {
    fn is_empty(&self) -> bool {
        self.ctype.is_none() && self.min.is_none() && self.max.is_none() && self.options.is_none()
    }

    /// Operator fields win over user fields.
    fn merged_with_user(&self, user: &Constraint) -> Constraint {
        Constraint {
            ctype: self.ctype.clone().or_else(|| user.ctype.clone()),
            min: self.min.or(user.min),
            max: self.max.or(user.max),
            options: self.options.clone().or_else(|| user.options.clone()),
        }
    }

    /// Whether `v` satisfies this constraint; `Err` explains the violation.
    fn validate(&self, v: &Value) -> Result<(), String> {
        if let Some(t) = &self.ctype {
            let ok = matches!(
                (t.as_str(), v),
                ("int", Value::Int(_))
                    | ("float", Value::Float(_))
                    | ("float", Value::Int(_)) // an int is a valid float
                    | ("string", Value::Str(_))
                    | ("bool", Value::Bool(_))
                    | ("options", Value::Str(_))
            );
            if !ok {
                return Err(format!("must be a `{t}` (got {})", v.type_name()));
            }
        }
        let num = match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        };
        if let Some(min) = self.min {
            match num {
                Some(n) if n >= min => {}
                Some(n) => return Err(format!("must be >= {min} (got {n})")),
                None => return Err("a `min` bound applies to numbers only".to_string()),
            }
        }
        if let Some(max) = self.max {
            match num {
                Some(n) if n <= max => {}
                Some(n) => return Err(format!("must be <= {max} (got {n})")),
                None => return Err("a `max` bound applies to numbers only".to_string()),
            }
        }
        if let Some(opts) = &self.options {
            match v {
                Value::Str(s) if opts.iter().any(|o| o == s) => {}
                Value::Str(s) => return Err(format!("must be one of [{}] (got `{s}`)", opts.join(", "))),
                _ => return Err("`options` applies to strings only".to_string()),
            }
        }
        Ok(())
    }

    fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(t) = &self.ctype {
            parts.push(format!("type={t}"));
        }
        if let Some(m) = self.min {
            parts.push(format!("min={m}"));
        }
        if let Some(m) = self.max {
            parts.push(format!("max={m}"));
        }
        if let Some(o) = &self.options {
            parts.push(format!("options={}", o.join("|")));
        }
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join(" ")
        }
    }
}

/// Infer a value from raw text when no `type` is declared.
fn infer_value(raw: &str) -> Value {
    let t = raw.trim();
    if let Ok(i) = t.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return Value::Float(f);
    }
    match t.to_ascii_lowercase().as_str() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => Value::Str(t.to_string()),
    }
}

/// Coerce raw text to a declared type.
fn coerce(raw: &str, ctype: &str) -> Result<Value, String> {
    let t = raw.trim();
    match ctype {
        "int" => t.parse::<i64>().map(Value::Int).map_err(|_| format!("`{t}` is not an int")),
        "float" => t.parse::<f64>().map(Value::Float).map_err(|_| format!("`{t}` is not a float")),
        "bool" => match t.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(Value::Bool(true)),
            "false" | "0" | "no" | "off" => Ok(Value::Bool(false)),
            _ => Err(format!("`{t}` is not a bool")),
        },
        "string" | "options" => Ok(Value::Str(t.to_string())),
        other => Err(format!("unknown type `{other}`")),
    }
}

// ── command parsing (pure, unit-tested) ─────────────────────────────────────

#[derive(Debug, PartialEq)]
enum StateCmd {
    Set { key: String, raw_value: String },
    Get { key: String },
    List,
    Remove { key: String },
    Define { key: String, constraint: Constraint },
    Help,
}

/// Parse the args after `!state`, returning the command + whether a leading
/// `@mention` was present (the caller resolves the target separately).
fn parse_state_command(args: &str) -> Option<(StateCmd, bool)> {
    // A mention may sit BEFORE the verb (`!state @user set k=v`) or, more
    // naturally, right AFTER it (`!state set @user k=v`).
    let (args, before) = strip_mention(args);
    let (head, rest0) = split_head(args);
    let (rest, after) = strip_mention(rest0);
    let had_mention = before || after;
    let cmd = match head.to_ascii_lowercase().as_str() {
        "" => return None,
        "help" => StateCmd::Help,
        "list" => StateCmd::List,
        "set" => {
            let (k, v) = rest.split_once('=')?;
            let key = k.trim();
            if key.is_empty() {
                return None;
            }
            StateCmd::Set { key: key.to_string(), raw_value: v.trim().to_string() }
        }
        "get" => {
            let key = rest.trim();
            if key.is_empty() {
                return None;
            }
            StateCmd::Get { key: key.to_string() }
        }
        "remove" | "delete" | "rm" => {
            let key = rest.trim();
            if key.is_empty() {
                return None;
            }
            StateCmd::Remove { key: key.to_string() }
        }
        "define" => parse_define(rest)?,
        _ => return None,
    };
    Some((cmd, had_mention))
}

fn parse_define(rest: &str) -> Option<StateCmd> {
    let (key, opts) = split_head(rest);
    if key.is_empty() {
        return None;
    }
    let mut c = Constraint::default();
    for tok in opts.split_whitespace() {
        let (k, v) = tok.split_once('=')?;
        match k.to_ascii_lowercase().as_str() {
            "type" => c.ctype = Some(v.to_ascii_lowercase()),
            "min" => c.min = v.parse::<f64>().ok(),
            "max" => c.max = v.parse::<f64>().ok(),
            "options" => c.options = Some(v.split('|').map(|s| s.trim().to_string()).collect()),
            _ => return None,
        }
    }
    Some(StateCmd::Define { key: key.to_string(), constraint: c })
}

/// Split the first whitespace-delimited token from the rest.
fn split_head(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.split_once(char::is_whitespace) {
        Some((h, r)) => (h, r.trim_start()),
        None => (s, ""),
    }
}

/// Drop a leading Discord mention token (`<@123>` / `<@!123>`), reporting it.
fn strip_mention(args: &str) -> (&str, bool) {
    let t = args.trim_start();
    if t.starts_with("<@") {
        if let Some(end) = t.find('>') {
            return (t[end + 1..].trim_start(), true);
        }
    }
    (t, false)
}

/// Parse the args after `!state` from a raw message, or None if not `!state`.
fn parse_state_args(raw: &str, flag: &str, name: &str) -> Option<String> {
    let t = raw.trim_start();
    let rest = t.strip_prefix(flag)?;
    let after = rest.trim_start();
    let head = after.get(..name.len())?;
    if !head.eq_ignore_ascii_case(name) {
        return None;
    }
    let tail = &after[name.len()..];
    match tail.chars().next() {
        None => Some(String::new()),
        Some(c) if c.is_whitespace() => Some(tail.trim_start().to_string()),
        _ => None,
    }
}

// ── discord raw_data helpers ────────────────────────────────────────────────

/// (id, handle) of the message author, from the Discord MESSAGE_CREATE JSON.
fn discord_author(raw_data: &[u8]) -> (String, String) {
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(raw_data) else {
        return (String::new(), String::new());
    };
    let a = v.get("author");
    let id = a.and_then(|a| a.get("id")).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let handle = a.and_then(|a| a.get("username")).and_then(|x| x.as_str()).unwrap_or("").to_string();
    (id, handle)
}

/// (id, handle) of the FIRST mentioned user, if any.
fn discord_mention(raw_data: &[u8]) -> Option<(String, String)> {
    let v: serde_json::Value = serde_json::from_slice(raw_data).ok()?;
    let m = v.get("mentions")?.as_array()?.first()?;
    let id = m.get("id").and_then(|x| x.as_str())?.to_string();
    let handle = m.get("username").and_then(|x| x.as_str()).unwrap_or("").to_string();
    Some((id, handle))
}

fn discord_guild(raw_data: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(raw_data)
        .ok()
        .and_then(|v| v.get("guild_id").and_then(|x| x.as_str()).map(|s| s.to_string()))
        .unwrap_or_default()
}

// ── store (turso) ───────────────────────────────────────────────────────────

fn vtext(s: &str) -> turso::Value {
    turso::Value::Text(s.to_string())
}
fn vint(i: i64) -> turso::Value {
    turso::Value::Integer(i)
}
fn vreal(f: f64) -> turso::Value {
    turso::Value::Real(f)
}
fn vnull() -> turso::Value {
    turso::Value::Null
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// One SQLite (turso) DB: a server's state + constraints, or the global set.
struct Store {
    conn: turso::Connection,
}

impl Store {
    async fn open(path: &str) -> Result<Self, String> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let db = turso::Builder::new_local(path).build().await.map_err(|e| format!("open {path}: {e}"))?;
        let conn = db.connect().map_err(|e| format!("connect {path}: {e}"))?;
        let s = Self { conn };
        s.ensure_schema().await?;
        Ok(s)
    }

    async fn ensure_schema(&self) -> Result<(), String> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS state (\
                 subject_platform TEXT NOT NULL, subject_id TEXT NOT NULL, \
                 skey TEXT NOT NULL, svalue TEXT NOT NULL, \
                 updated_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (subject_platform, subject_id, skey))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS constraints (\
                 skey TEXT PRIMARY KEY, ctype TEXT, cmin REAL, cmax REAL, \
                 coptions TEXT, updated_at INTEGER NOT NULL DEFAULT 0)",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn count_keys(&self, platform: &str, subject: &str) -> Result<usize, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM state WHERE subject_platform = ?1 AND subject_id = ?2",
                turso::params![vtext(platform), vtext(subject)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let n = match rows.next().await.map_err(|e| e.to_string())? {
            Some(r) => r.get::<i64>(0).unwrap_or(0),
            None => 0,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(n as usize)
    }

    async fn get(&self, platform: &str, subject: &str, key: &str) -> Result<Option<Value>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT svalue FROM state WHERE subject_platform = ?1 AND subject_id = ?2 AND skey = ?3",
                turso::params![vtext(platform), vtext(subject), vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = match rows.next().await.map_err(|e| e.to_string())? {
            Some(r) => r.get::<String>(0).ok().and_then(|s| Value::from_json(&s)),
            None => None,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    async fn set(&self, platform: &str, subject: &str, key: &str, value: &Value) -> Result<(), String> {
        // DELETE + INSERT (turso 0.1.0 has no upsert).
        self.conn
            .execute(
                "DELETE FROM state WHERE subject_platform = ?1 AND subject_id = ?2 AND skey = ?3",
                turso::params![vtext(platform), vtext(subject), vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO state (subject_platform, subject_id, skey, svalue, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                turso::params![vtext(platform), vtext(subject), vtext(key), vtext(&value.to_json()), vint(now_ms())],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn remove(&self, platform: &str, subject: &str, key: &str) -> Result<bool, String> {
        let before = self.get(platform, subject, key).await?;
        self.conn
            .execute(
                "DELETE FROM state WHERE subject_platform = ?1 AND subject_id = ?2 AND skey = ?3",
                turso::params![vtext(platform), vtext(subject), vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(before.is_some())
    }

    async fn list(&self, platform: &str, subject: &str) -> Result<Vec<(String, Value)>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT skey, svalue FROM state WHERE subject_platform = ?1 AND subject_id = ?2 ORDER BY skey ASC",
                turso::params![vtext(platform), vtext(subject)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            if let (Ok(k), Ok(v)) = (r.get::<String>(0), r.get::<String>(1)) {
                if let Some(val) = Value::from_json(&v) {
                    out.push((k, val));
                }
            }
        }
        Ok(out)
    }

    async fn set_constraint(&self, key: &str, c: &Constraint) -> Result<(), String> {
        let opts = c.options.as_ref().map(|o| serde_json::to_string(o).unwrap());
        self.conn
            .execute(
                "DELETE FROM constraints WHERE skey = ?1",
                turso::params![vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO constraints (skey, ctype, cmin, cmax, coptions, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                turso::params![
                    vtext(key),
                    c.ctype.as_deref().map(vtext).unwrap_or_else(vnull),
                    c.min.map(vreal).unwrap_or_else(vnull),
                    c.max.map(vreal).unwrap_or_else(vnull),
                    opts.as_deref().map(vtext).unwrap_or_else(vnull),
                    vint(now_ms())
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn get_constraint(&self, key: &str) -> Result<Option<Constraint>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT ctype, cmin, cmax, coptions FROM constraints WHERE skey = ?1",
                turso::params![vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = rows.next().await.map_err(|e| e.to_string())?.map(|r| Constraint {
            ctype: r.get::<Option<String>>(0).ok().flatten(),
            min: r.get::<Option<f64>>(1).ok().flatten(),
            max: r.get::<Option<f64>>(2).ok().flatten(),
            options: r
                .get::<Option<String>>(3)
                .ok()
                .flatten()
                .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok()),
        });
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }
}

/// Per-guild stores, opened lazily (the read task owns them).
struct Stores {
    dir: String,
    by_guild: HashMap<String, Store>,
}

impl Stores {
    fn new(dir: String) -> Self {
        Self { dir, by_guild: HashMap::new() }
    }

    fn path_for(key: &str, dir: &str) -> String {
        format!("{}/{key}.db", expand_tilde(dir).trim_end_matches('/'))
    }

    async fn guild(&mut self, key: &str) -> Result<&Store, String> {
        if !self.by_guild.contains_key(key) {
            let store = Store::open(&Self::path_for(key, &self.dir)).await?;
            self.by_guild.insert(key.to_string(), store);
        }
        Ok(self.by_guild.get(key).unwrap())
    }
}

// ── engine plumbing ─────────────────────────────────────────────────────────

#[derive(Clone)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

async fn send_container(write: &Arc<AsyncMutex<WsWriteHalf>>, c: ContainerForEngine) {
    let mut buf = Vec::new();
    if c.encode(&mut buf).is_ok() {
        let mut w = write.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

async fn register_catch_all(write: &Arc<AsyncMutex<WsWriteHalf>>, id: &Arc<AsyncMutex<EngineIdentity>>) {
    let id = id.lock().await.clone();
    let c = ContainerForEngine {
        version: 2,
        auth_token: id.auth,
        module_name: id.module,
        module_instance_uuid7: id.instance,
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![],
            alert_on_unknown_command: false,
        })),
    };
    send_container(write, c).await;
    info!("registered as a catch-all (parses !state itself)");
}

fn reply_container(id: &EngineIdentity, chat: &ChatMessage, msg: String) -> ContainerForEngine {
    let handle = chat.user_data.as_ref().map(|u| u.username.clone()).unwrap_or_default();
    let send = SendToPlatforms {
        msg,
        level: 0,
        module_uuid7: id.instance.clone(),
        pid: String::new(),
        platform: chat.platform.clone(),
        actor_platform: chat.platform.clone(),
        actor_handle: handle,
        actor_uuid7: chat.user_uuid7.clone(),
        channel_id: chat.channel_id.clone(),
        ..Default::default()
    };
    ContainerForEngine {
        version: 2,
        auth_token: id.auth.clone(),
        module_name: id.module.clone(),
        module_instance_uuid7: id.instance.clone(),
        payload: Some(EnginePayload::SendToPlatforms(send)),
    }
}

// ── command handling ────────────────────────────────────────────────────────

async fn handle_command(
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    settings: &Settings,
    guild: &str,
    target: (String, String), // (subject_id, handle)
    cmd: StateCmd,
) -> Vec<ContainerForEngine> {
    let mut out = Vec::new();
    macro_rules! push {
        ($m:expr) => {
            out.push(reply_container(id, chat, $m))
        };
    }
    let platform = chat.platform.clone();
    let (subject, handle) = target;
    let who = if handle.is_empty() { "that user".to_string() } else { format!("@{handle}") };

    match cmd {
        StateCmd::Help => {
            push!(format!(
                "`{f}state set [@user] <key>=<value>` · `{f}state get [@user] <key>` · `{f}state list [@user]` · `{f}state remove [@user] <key>` · `{f}state define <key> [type=int|float|string|bool|options] [min=..] [max=..] [options=a|b|c]`",
                f = settings.command_flag
            ));
        }
        StateCmd::List => match stores.guild(guild).await {
            Ok(s) => match s.list(&platform, &subject).await {
                Ok(items) if items.is_empty() => push!(format!("{who} has no properties.")),
                Ok(items) => {
                    let body = items
                        .iter()
                        .map(|(k, v)| format!("`{k}`={}", v.display()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    push!(format!("{who}: {body}"));
                }
                Err(e) => push!(format!("storage error: {e}")),
            },
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Get { key } => match stores.guild(guild).await {
            Ok(s) => match s.get(&platform, &subject, &key).await {
                Ok(Some(v)) => push!(format!("{who} `{key}` = {}", v.display())),
                Ok(None) => push!(format!("{who} has no property `{key}`.")),
                Err(e) => push!(format!("storage error: {e}")),
            },
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Set { key, raw_value } => {
            if key.len() > MAX_KEY_LEN {
                push!(format!("key is too long (max {MAX_KEY_LEN})."));
                return out;
            }
            if raw_value.len() > MAX_VALUE_LEN {
                push!(format!("value is too long (max {MAX_VALUE_LEN})."));
                return out;
            }
            // Effective constraint (operator wins over user-defined).
            let op = settings.schema.get(&key).cloned().unwrap_or_default();
            let user = match stores.guild(guild).await {
                Ok(s) => s.get_constraint(&key).await.unwrap_or(None).unwrap_or_default(),
                Err(_) => Constraint::default(),
            };
            let con = op.merged_with_user(&user);

            // Coerce to the declared type, else infer.
            let value = match con.ctype.as_deref() {
                Some(t) => match coerce(&raw_value, t) {
                    Ok(v) => v,
                    Err(e) => {
                        push!(format!("`{key}`: {e}."));
                        return out;
                    }
                },
                None => infer_value(&raw_value),
            };
            if let Err(e) = con.validate(&value) {
                push!(format!("`{key}`: {e}."));
                return out;
            }
            // Cap the number of keys per subject.
            if let Ok(s) = stores.guild(guild).await {
                if s.get(&platform, &subject, &key).await.unwrap_or(None).is_none()
                    && s.count_keys(&platform, &subject).await.unwrap_or(0) >= MAX_KEYS_PER_SUBJECT
                {
                    push!(format!("{who} already has {MAX_KEYS_PER_SUBJECT} properties (the cap)."));
                    return out;
                }
                match s.set(&platform, &subject, &key, &value).await {
                    Ok(()) => push!(format!("Set `{key}` = {} for {who}.", value.display())),
                    Err(e) => push!(format!("storage error: {e}")),
                }
            } else {
                push!("storage error".to_string());
            }
        }
        StateCmd::Remove { key } => match stores.guild(guild).await {
            Ok(s) => match s.remove(&platform, &subject, &key).await {
                Ok(true) => push!(format!("Removed `{key}` from {who}.")),
                Ok(false) => push!(format!("{who} has no property `{key}`.")),
                Err(e) => push!(format!("storage error: {e}")),
            },
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Define { key, constraint } => {
            if constraint.is_empty() {
                push!("Nothing to define — pass type/min/max/options.".to_string());
                return out;
            }
            if let Some(t) = &constraint.ctype {
                if !["int", "float", "string", "bool", "options"].contains(&t.as_str()) {
                    push!(format!("unknown type `{t}` (int|float|string|bool|options)."));
                    return out;
                }
            }
            match stores.guild(guild).await {
                Ok(s) => match s.set_constraint(&key, &constraint).await {
                    Ok(()) => push!(format!("Constraint on `{key}`: {}.", constraint.describe())),
                    Err(e) => push!(format!("storage error: {e}")),
                },
                Err(e) => push!(format!("storage error: {e}")),
            }
        }
    }
    out
}

/// Handle one message: a `!state` command (else nothing).
async fn handle_message(
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    settings: &Settings,
) -> Vec<ContainerForEngine> {
    if chat.user_uuid7.is_empty() {
        return Vec::new();
    }
    let Some(args) = parse_state_args(&chat.raw_message, &settings.command_flag, STATE_COMMAND) else {
        return Vec::new();
    };
    let Some((cmd, had_mention)) = parse_state_command(&args) else {
        return Vec::new();
    };
    let guild = discord_guild(&chat.raw_data);
    let guild_key = if guild.is_empty() { DM_DB.to_string() } else { guild };

    // Target = the first mention (if the command used one), else the sender.
    let target = if had_mention {
        discord_mention(&chat.raw_data).unwrap_or_else(|| discord_author(&chat.raw_data))
    } else {
        discord_author(&chat.raw_data)
    };
    if target.0.is_empty() {
        return Vec::new();
    }
    handle_command(id, chat, stores, settings, &guild_key, target, cmd).await
}

// ── main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let module_name = client.config.module_name.clone();
    info!("state module connected as '{}'", module_name);
    let identity: Arc<AsyncMutex<EngineIdentity>> = Arc::new(AsyncMutex::new(EngineIdentity {
        auth: client.auth_token.clone(),
        instance: client.instance_uuid7.clone(),
        module: module_name,
    }));

    let settings = ensure_defaults();
    let write_for_task = Arc::clone(&write_shared);
    let identity_for_task = Arc::clone(&identity);

    tokio::spawn(async move {
        let mut read = read;
        let mut stores = Stores::new(settings.db_dir.clone());
        loop {
            register_catch_all(&write_for_task, &identity_for_task).await;
            loop {
                let Some(msg) = read.next().await else { break };
                let data = match msg {
                    Ok(WsMessage::Binary(d)) => d,
                    Ok(WsMessage::Close(_)) => break,
                    Ok(_) => continue,
                    Err(e) => {
                        warn!("Engine WebSocket error: {}", e);
                        break;
                    }
                };
                let Ok(container) = ContainerForModule::decode(data.as_ref()) else { continue };
                let id = identity_for_task.lock().await.clone();
                match container.payload {
                    Some(ModulePayload::AuthVerify(_)) => {
                        let reply = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::AuthVerify(AuthVerify { cur_auth: id.auth.clone() })),
                        };
                        send_container(&write_for_task, reply).await;
                    }
                    Some(ModulePayload::MessageInProcess(process)) => {
                        if !process.message_uuid7.is_empty() {
                            let receipt = ContainerForEngine {
                                version: 2,
                                auth_token: id.auth.clone(),
                                module_name: id.module.clone(),
                                module_instance_uuid7: id.instance.clone(),
                                payload: Some(EnginePayload::MessageAck(MessageAck {
                                    message_uuid7: process.message_uuid7.clone(),
                                })),
                            };
                            send_container(&write_for_task, receipt).await;
                        }
                        if let Some(chat) = &process.raw_message {
                            for c in handle_message(&id, chat, &mut stores, &settings).await {
                                send_container(&write_for_task, c).await;
                            }
                        }
                        let ack = ContainerForEngine {
                            version: 2,
                            auth_token: id.auth.clone(),
                            module_name: id.module.clone(),
                            module_instance_uuid7: id.instance.clone(),
                            payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
                                message_uuid7: process.message_uuid7,
                                raw_message: process.raw_message,
                                processed_message: process.processed_message,
                                abandon_message: process.abandon_message,
                                audio: process.audio,
                                audio_type: process.audio_type,
                            })),
                        };
                        send_container(&write_for_task, ack).await;
                    }
                    Some(ModulePayload::Shutdown(_)) => {
                        info!("Engine requested shutdown — exiting");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }

            info!("Engine disconnected — reconnecting...");
            let mut backoff = settings.reconnect_base_secs;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_for_task.lock().await = w;
                        *identity_for_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        read = r;
                        break;
                    }
                    Err(e) => {
                        warn!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(settings.reconnect_max_secs);
                    }
                }
            }
        }
    });

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_value_types() {
        assert_eq!(infer_value("42"), Value::Int(42));
        assert_eq!(infer_value("-3"), Value::Int(-3));
        assert_eq!(infer_value("4.2"), Value::Float(4.2));
        assert_eq!(infer_value("true"), Value::Bool(true));
        assert_eq!(infer_value("hello"), Value::Str("hello".into()));
    }

    #[test]
    fn coerces_to_declared_type() {
        assert_eq!(coerce("42", "int").unwrap(), Value::Int(42));
        assert!(coerce("x", "int").is_err());
        assert_eq!(coerce("4.2", "float").unwrap(), Value::Float(4.2));
        assert_eq!(coerce("yes", "bool").unwrap(), Value::Bool(true));
        assert_eq!(coerce("42", "string").unwrap(), Value::Str("42".into()));
        assert!(coerce("42", "nope").is_err());
    }

    #[test]
    fn validates_type_min_max_options() {
        let c = Constraint { ctype: Some("int".into()), min: Some(0.0), max: Some(100.0), options: None };
        assert!(c.validate(&Value::Int(50)).is_ok());
        assert!(c.validate(&Value::Int(0)).is_ok());
        assert!(c.validate(&Value::Int(100)).is_ok());
        assert!(c.validate(&Value::Int(101)).is_err());
        assert!(c.validate(&Value::Int(-1)).is_err());
        assert!(c.validate(&Value::Str("x".into())).is_err());

        let opts = Constraint { ctype: Some("options".into()), options: Some(vec!["a".into(), "b".into()]), ..Default::default() };
        assert!(opts.validate(&Value::Str("a".into())).is_ok());
        assert!(opts.validate(&Value::Str("c".into())).is_err());

        // int is an acceptable float
        let f = Constraint { ctype: Some("float".into()), ..Default::default() };
        assert!(f.validate(&Value::Int(3)).is_ok());
        assert!(f.validate(&Value::Float(3.5)).is_ok());
        assert!(f.validate(&Value::Str("3".into())).is_err());
    }

    #[test]
    fn operator_constraint_wins_over_user() {
        let op = Constraint { ctype: Some("int".into()), min: Some(0.0), ..Default::default() };
        let user = Constraint { ctype: Some("string".into()), min: Some(5.0), max: Some(9.0), ..Default::default() };
        let m = op.merged_with_user(&user);
        assert_eq!(m.ctype.as_deref(), Some("int")); // operator wins
        assert_eq!(m.min, Some(0.0)); // operator wins
        assert_eq!(m.max, Some(9.0)); // user fills the gap
    }

    #[test]
    fn value_json_round_trips() {
        for v in [Value::Int(7), Value::Float(1.5), Value::Bool(true), Value::Str("hi".into())] {
            assert_eq!(Value::from_json(&v.to_json()).unwrap(), v);
        }
    }

    #[test]
    fn parses_commands() {
        assert_eq!(parse_state_command("list"), Some((StateCmd::List, false)));
        assert_eq!(
            parse_state_command("set score=42"),
            Some((StateCmd::Set { key: "score".into(), raw_value: "42".into() }, false))
        );
        assert_eq!(
            parse_state_command("set @x y=1"),
            Some((StateCmd::Set { key: "@x y".into(), raw_value: "1".into() }, false))
        );
        // A mention BEFORE the verb is stripped + reported.
        assert_eq!(
            parse_state_command("<@123> get score"),
            Some((StateCmd::Get { key: "score".into() }, true))
        );
        // A mention AFTER the verb (the natural form) too.
        assert_eq!(
            parse_state_command("set <@2> nickname=Bob"),
            Some((StateCmd::Set { key: "nickname".into(), raw_value: "Bob".into() }, true))
        );
        assert_eq!(
            parse_state_command("get <@2> score"),
            Some((StateCmd::Get { key: "score".into() }, true))
        );
        assert_eq!(
            parse_state_command("remove <@2> score"),
            Some((StateCmd::Remove { key: "score".into() }, true))
        );
        assert_eq!(parse_state_command("get score"), Some((StateCmd::Get { key: "score".into() }, false)));
        assert_eq!(
            parse_state_command("remove score"),
            Some((StateCmd::Remove { key: "score".into() }, false))
        );
        assert_eq!(
            parse_state_command("define score type=int min=0 max=100"),
            Some((
                StateCmd::Define {
                    key: "score".into(),
                    constraint: Constraint { ctype: Some("int".into()), min: Some(0.0), max: Some(100.0), options: None }
                },
                false
            ))
        );
        assert_eq!(
            parse_state_command("define colour type=options options=red|green|blue"),
            Some((
                StateCmd::Define {
                    key: "colour".into(),
                    constraint: Constraint {
                        ctype: Some("options".into()),
                        options: Some(vec!["red".into(), "green".into(), "blue".into()]),
                        ..Default::default()
                    }
                },
                false
            ))
        );
        assert_eq!(parse_state_command("help"), Some((StateCmd::Help, false)));
        assert_eq!(parse_state_command(""), None);
        assert_eq!(parse_state_command("bogus"), None);
        assert_eq!(parse_state_command("set noequals"), None);
    }

    #[test]
    fn parses_state_args_only_for_the_command() {
        assert_eq!(parse_state_args("!state list", "!", "state").as_deref(), Some("list"));
        assert_eq!(parse_state_args("!state", "!", "state").as_deref(), Some(""));
        assert!(parse_state_args("!statex", "!", "state").is_none());
        assert!(parse_state_args("hello", "!", "state").is_none());
    }

    #[test]
    fn parses_operator_schema() {
        let v = serde_json::json!({"score": {"type":"int","min":0,"max":100}});
        let s = parse_schema(&v);
        let c = s.get("score").unwrap();
        assert_eq!(c.ctype.as_deref(), Some("int"));
        assert_eq!(c.min, Some(0.0));
        assert_eq!(c.max, Some(100.0));
    }

    // ── end-to-end store flow ───────────────────────────────────────────────

    fn discord_msg(user_id: &str, handle: &str, channel: &str, guild: &str, text: &str) -> ChatMessage {
        let raw = serde_json::json!({
            "id": "m1",
            "guild_id": guild,
            "channel_id": channel,
            "content": text,
            "author": {"id": user_id, "username": handle},
            "mentions": []
        })
        .to_string();
        ChatMessage {
            platform: "discord".into(),
            raw_data: raw.into_bytes(),
            raw_message: text.into(),
            user_uuid7: format!("u-{user_id}"),
            command: None,
            user_data: Some(UserData { username: handle.into(), ..Default::default() }),
            channel_id: channel.into(),
        }
    }

    fn settings() -> Settings {
        Settings {
            command_flag: "!".into(),
            db_dir: String::new(),
            reconnect_base_secs: 1,
            reconnect_max_secs: 30,
            schema: HashMap::new(),
        }
    }

    fn test_id() -> EngineIdentity {
        EngineIdentity { auth: "t".into(), instance: "i".into(), module: "state".into() }
    }

    fn reply_texts(v: &[ContainerForEngine]) -> Vec<String> {
        v.iter()
            .filter_map(|c| match &c.payload {
                Some(EnginePayload::SendToPlatforms(s)) => Some(s.msg.clone()),
                _ => None,
            })
            .collect()
    }

    /// Play a message from alice (id "1") in guild "g1" / channel "c1".
    async fn alice_says(stores: &mut Stores, settings: &Settings, text: &str) -> Vec<ContainerForEngine> {
        let chat = discord_msg("1", "alice", "c1", "g1", text);
        handle_message(&test_id(), &chat, stores, settings).await
    }

    #[tokio::test]
    async fn state_flow() {
        let dir = std::env::temp_dir().join(format!("cockatiel-state-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let id = test_id();
        let mut set = settings();
        set.schema.insert(
            "score".into(),
            Constraint { ctype: Some("int".into()), min: Some(0.0), max: Some(100.0), options: None },
        );
        let (guild, channel) = ("g1", "c1");

        // set an int within the operator constraint
        let out = alice_says(&mut stores, &set, "!state set score=42").await;
        assert!(reply_texts(&out)[0].contains("Set `score` = 42"), "{:?}", reply_texts(&out));
        assert_eq!(stores.guild(guild).await.unwrap().get("discord", "1", "score").await.unwrap(), Some(Value::Int(42)));

        // out of range → rejected, unchanged
        let out = alice_says(&mut stores, &set, "!state set score=999").await;
        assert!(reply_texts(&out)[0].contains("must be <= 100"), "{:?}", reply_texts(&out));
        assert_eq!(stores.guild(guild).await.unwrap().get("discord", "1", "score").await.unwrap(), Some(Value::Int(42)));

        // non-int → rejected
        let out = alice_says(&mut stores, &set, "!state set score=abc").await;
        assert!(reply_texts(&out)[0].contains("not an int"), "{:?}", reply_texts(&out));

        // user-defined constraint on a new key (options)
        let out = alice_says(&mut stores, &set, "!state define colour type=options options=red|green|blue").await;
        assert!(reply_texts(&out)[0].contains("Constraint on `colour`"));
        let out = alice_says(&mut stores, &set, "!state set colour=purple").await;
        assert!(reply_texts(&out)[0].contains("must be one of"), "{:?}", reply_texts(&out));
        let out = alice_says(&mut stores, &set, "!state set colour=green").await;
        assert!(reply_texts(&out)[0].contains("Set `colour` = green"));

        // get + list
        let out = alice_says(&mut stores, &set, "!state get colour").await;
        assert!(reply_texts(&out)[0].contains("`colour` = green"));
        let out = alice_says(&mut stores, &set, "!state list").await;
        assert!(reply_texts(&out)[0].contains("`score`=42") && reply_texts(&out)[0].contains("`colour`=green"));

        // anyone can edit anyone's: alice sets a value on bob via a mention
        let mut bob_msg = discord_msg("1", "alice", channel, guild, "!state set <@2> nickname=Bob");
        bob_msg.raw_data = serde_json::json!({
            "id": "m2", "guild_id": guild, "channel_id": channel,
            "content": "!state set <@2> nickname=Bob",
            "author": {"id": "1", "username": "alice"},
            "mentions": [{"id": "2", "username": "bob"}]
        }).to_string().into_bytes();
        let out = handle_message(&id, &bob_msg, &mut stores, &set).await;
        assert!(reply_texts(&out)[0].contains("Set `nickname` = Bob for @bob"), "{:?}", reply_texts(&out));
        assert_eq!(stores.guild(guild).await.unwrap().get("discord", "2", "nickname").await.unwrap(), Some(Value::Str("Bob".into())));

        // remove
        let out = alice_says(&mut stores, &set, "!state remove colour").await;
        assert!(reply_texts(&out)[0].contains("Removed `colour`"));
        let out = alice_says(&mut stores, &set, "!state get colour").await;
        assert!(reply_texts(&out)[0].contains("no property `colour`"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
