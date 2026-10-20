//! state — per-character stats with categories, optional constraints, and
//! formula-driven per-turn modifiers.
//!
//! A **stat** belongs to a **character** (an RP character name, e.g. "alice")
//! and has a **category** (for grouping), a **name** (the key) and a typed
//! **value**. Anyone may read or edit any character's stats.
//!
//! Explicit, flag-based commands (the `-char` names the target character):
//!
//!   !state stat   -char <c> [-c <category>] -n <name> -v <value>
//!   !state modify -char <c> -k <name> -v <value>            set a value
//!   !state modify -char <c> -k <name> -m <formula> [-t <n>] [-name <mname>]
//!                                                           add/replace a modifier
//!   !state list   -char <c> [-c <category>]
//!   !state remove -char <c> -k <name>       remove one stat
//!   !state remove -char <c> -c <category>   remove a whole category
//!   !state help
//!
//! A **modifier** changes its stat by a formula **every turn** (a turn = one
//! ordinary message). In the formula, `t` is the modifier's turn number and any
//! stat name is a variable (its current value), e.g. `-m t*2` or `-m hp/10 + 1`.
//! `-t <n>` sets a finite turn budget; omitted / `inf` / a non-positive number
//! means indefinite. The result is clamped to the key's constraint.
//!
//! Values are typed (int / float / string / bool / options) and may be
//! constrained by `type`, `min`/`max`, and `options` — from the operator schema
//! (`config.json` → `module_specific.schema`) and from `!state` … merges (the
//! operator wins). Without a `type` the value's type is inferred from the text.
//!
//! Storage: one `turso` DB per server (`<db_dir>/<guild_id>.db`).

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
const DEFAULT_CATEGORY: &str = "general";
const MAX_NAME_LEN: usize = 64;
const MAX_VALUE_LEN: usize = 1024;
const MAX_STATS_PER_CHAR: usize = 200;

// ── randomness (dependency-free xorshift) ───────────────────────────────────

use std::sync::atomic::{AtomicU64, Ordering};
static RNG_STATE: AtomicU64 = AtomicU64::new(0);

/// A fast xorshift64 PRNG. Seeded lazily from the clock; good enough for dice.
fn next_rand() -> u64 {
    let mut x = RNG_STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9e37_79b9_7f4a_7c15)
            | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    RNG_STATE.store(x, Ordering::Relaxed);
    x
}

/// Roll one die with `sides` faces → 1..=sides (0 if `sides` is 0).
fn roll_die(sides: u32) -> i64 {
    if sides == 0 {
        0
    } else {
        (next_rand() % sides as u64) as i64 + 1
    }
}

/// A random integer in `[lo, hi]` (inclusive); swapped if reversed.
fn roll_range(lo: i64, hi: i64) -> i64 {
    let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
    let span = (hi - lo) as u64 + 1;
    lo + (next_rand() % span) as i64
}

// ── config ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct Settings {
    command_flag: String,
    db_dir: String,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
    /// Operator constraints: key -> Constraint.
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
            if let Some(ms) = obj.entry("module_specific".to_string()).or_insert_with(|| serde_json::json!({})).as_object_mut() {
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

fn parse_schema(v: &serde_json::Value) -> HashMap<String, Constraint> {
    let mut out = HashMap::new();
    if let Some(obj) = v.as_object() {
        for (k, c) in obj {
            out.insert(
                k.clone(),
                Constraint {
                    ctype: c.get("type").and_then(|x| x.as_str()).map(|s| s.to_ascii_lowercase()),
                    min: c.get("min").and_then(|x| x.as_f64()),
                    max: c.get("max").and_then(|x| x.as_f64()),
                    options: c.get("options").and_then(|x| x.as_array()).map(|a| a.iter().filter_map(|s| s.as_str().map(|s| s.to_string())).collect()),
                },
            );
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

// ── value + constraint (pure) ───────────────────────────────────────────────

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
    fn display(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Str(s) => s.clone(),
        }
    }
    /// Numeric view (for formulas / modifiers); None for strings/bools.
    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Constraint {
    ctype: Option<String>,
    min: Option<f64>,
    max: Option<f64>,
    options: Option<Vec<String>>,
}

impl Constraint {
    fn is_empty(&self) -> bool {
        self.ctype.is_none() && self.min.is_none() && self.max.is_none() && self.options.is_none()
    }
    fn merged_with_user(&self, user: &Constraint) -> Constraint {
        Constraint {
            ctype: self.ctype.clone().or_else(|| user.ctype.clone()),
            min: self.min.or(user.min),
            max: self.max.or(user.max),
            options: self.options.clone().or_else(|| user.options.clone()),
        }
    }
    fn validate(&self, v: &Value) -> Result<(), String> {
        if let Some(t) = &self.ctype {
            let ok = matches!(
                (t.as_str(), v),
                ("int", Value::Int(_))
                    | ("float", Value::Float(_))
                    | ("float", Value::Int(_))
                    | ("string", Value::Str(_))
                    | ("bool", Value::Bool(_))
                    | ("options", Value::Str(_))
            );
            if !ok {
                return Err(format!("must be a `{t}` (got {})", v.type_name()));
            }
        }
        let num = v.as_f64();
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
        let mut p = Vec::new();
        if let Some(t) = &self.ctype {
            p.push(format!("type={t}"));
        }
        if let Some(m) = self.min {
            p.push(format!("min={m}"));
        }
        if let Some(m) = self.max {
            p.push(format!("max={m}"));
        }
        if let Some(o) = &self.options {
            p.push(format!("options={}", o.join("|")));
        }
        if p.is_empty() {
            "none".to_string()
        } else {
            p.join(" ")
        }
    }
}

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

/// Parse a turn count: `7`/`x7` finite; omitted/non-positive/`inf` → -1 (forever).
fn parse_turns(s: &str) -> Option<i64> {
    let t = s.trim().trim_start_matches(['x', 'X']).trim();
    if t.eq_ignore_ascii_case("inf") || t.eq_ignore_ascii_case("infinite") || t.eq_ignore_ascii_case("forever") {
        return Some(-1);
    }
    Some(match t.parse::<i64>().ok()? {
        n if n <= 0 => -1,
        n => n,
    })
}

// ── formula evaluator (pure) ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Var(String),
    Op(char),
    LP,
    RP,
    Comma,
}

fn tokenize(expr: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = expr.chars().collect();
    let mut toks = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit() || c == '.' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let n: f64 = chars[start..i].iter().collect::<String>().parse().map_err(|_| "bad number".to_string())?;
            toks.push(Tok::Num(n));
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            toks.push(Tok::Var(chars[start..i].iter().collect()));
        } else {
            toks.push(match c {
                '+' | '-' | '*' | '/' => Tok::Op(c),
                '(' => Tok::LP,
                ')' => Tok::RP,
                ',' => Tok::Comma,
                _ => return Err(format!("unexpected `{c}`")),
            });
            i += 1;
        }
    }
    Ok(toks)
}

struct Parser<'a> {
    toks: &'a [Tok],
    pos: usize,
    vars: &'a HashMap<String, f64>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn expr(&mut self) -> Result<f64, String> {
        let mut v = self.term()?;
        while let Some(Tok::Op(op @ ('+' | '-'))) = self.peek() {
            let op = *op;
            self.pos += 1;
            let r = self.term()?;
            v = if op == '+' { v + r } else { v - r };
        }
        Ok(v)
    }
    fn term(&mut self) -> Result<f64, String> {
        let mut v = self.factor()?;
        while let Some(Tok::Op(op @ ('*' | '/'))) = self.peek() {
            let op = *op;
            self.pos += 1;
            let r = self.factor()?;
            if op == '/' {
                if r == 0.0 {
                    return Err("division by zero".to_string());
                }
                v /= r;
            } else {
                v *= r;
            }
        }
        Ok(v)
    }
    fn factor(&mut self) -> Result<f64, String> {
        match self.peek().cloned() {
            Some(Tok::Num(n)) => {
                self.pos += 1;
                Ok(n)
            }
            Some(Tok::Var(name)) => {
                self.pos += 1;
                // A function call: name(args…).
                if matches!(self.peek(), Some(Tok::LP)) {
                    self.pos += 1;
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Some(Tok::RP)) {
                        loop {
                            args.push(self.expr()?);
                            match self.peek() {
                                Some(Tok::Comma) => self.pos += 1,
                                _ => break,
                            }
                        }
                    }
                    if !matches!(self.peek(), Some(Tok::RP)) {
                        return Err("missing `)`".to_string());
                    }
                    self.pos += 1;
                    return call_fn(&name, &args);
                }
                // A die literal: `d20` / `D6` → 1..=n.
                if let Some(rest) = name.strip_prefix(['d', 'D']) {
                    if let Ok(sides) = rest.parse::<u32>() {
                        return Ok(roll_die(sides) as f64);
                    }
                }
                self.vars.get(&name.to_ascii_lowercase()).copied().ok_or_else(|| format!("unknown name `{name}`"))
            }
            Some(Tok::Op('-')) => {
                self.pos += 1;
                Ok(-self.factor()?)
            }
            Some(Tok::Op('+')) => {
                self.pos += 1;
                self.factor()
            }
            Some(Tok::LP) => {
                self.pos += 1;
                let v = self.expr()?;
                match self.peek() {
                    Some(Tok::RP) => {
                        self.pos += 1;
                        Ok(v)
                    }
                    _ => Err("missing `)`".to_string()),
                }
            }
            other => Err(format!("unexpected {other:?}")),
        }
    }
}

/// Built-in formula functions (dice/random + small math).
fn call_fn(name: &str, args: &[f64]) -> Result<f64, String> {
    match (name.to_ascii_lowercase().as_str(), args) {
        ("rand", [a, b]) | ("random", [a, b]) => Ok(roll_range(a.ceil() as i64, b.floor() as i64) as f64),
        ("d", [n]) => Ok(roll_die(n.max(0.0) as u32) as f64),
        ("min", [a, b]) => Ok(a.min(*b)),
        ("max", [a, b]) => Ok(a.max(*b)),
        ("floor", [a]) => Ok(a.floor()),
        ("ceil", [a]) => Ok(a.ceil()),
        ("round", [a]) => Ok(a.round()),
        ("abs", [a]) => Ok(a.abs()),
        _ => Err(format!("unknown function `{name}` (rand/min/max/floor/ceil/round/abs/d)")),
    }
}

/// Evaluate a formula with the given variables (names lower-cased).
fn eval_formula(expr: &str, vars: &HashMap<String, f64>) -> Result<f64, String> {
    let toks = tokenize(expr)?;
    if toks.is_empty() {
        return Err("empty formula".to_string());
    }
    let mut p = Parser { toks: &toks, pos: 0, vars };
    let v = p.expr()?;
    if p.pos != p.toks.len() {
        return Err("trailing input".to_string());
    }
    Ok(v)
}

// ── stat + modifier ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct Stat {
    category: String,
    name: String,
    value: Value,
}

#[derive(Debug, Clone, PartialEq)]
struct Modifier {
    name: String,
    key: String,
    expr: String,
    /// Remaining turns; -1 = indefinite.
    turns: i64,
    /// Turns already applied (so `t` in the formula is `ticked + 1`).
    ticked: i64,
}

fn describe_modifier(m: &Modifier) -> String {
    let dur = if m.turns < 0 { "∞".to_string() } else { format!("{} turn(s)", m.turns) };
    format!("`{}` {}(={}) to `{}` ({dur})", m.name, m.expr, m.ticked + 1, m.key)
}

/// A threshold trigger: when a stat crosses `val` (direction `op`), announce
/// `message`.
#[derive(Debug, Clone, PartialEq)]
struct Trigger {
    op: String,
    val: f64,
    message: String,
}

impl Trigger {
    /// Whether `v` satisfies the condition (None = the stat is unset → no).
    fn satisfied(&self, v: Option<f64>) -> bool {
        let Some(n) = v else { return false };
        match self.op.as_str() {
            "le" | "<=" => n <= self.val,
            "ge" | ">=" => n >= self.val,
            "lt" | "<" => n < self.val,
            "gt" | ">" => n > self.val,
            "eq" | "==" | "=" => (n - self.val).abs() < 1e-9,
            "ne" | "!=" => (n - self.val).abs() >= 1e-9,
            _ => false,
        }
    }
}

/// One recorded stat change (the change log).
#[derive(Debug, Clone, PartialEq)]
struct Change {
    key: String,
    old: Option<String>,
    new: String,
    actor: String,
    source: String,
    at: i64,
}

// ── flags ───────────────────────────────────────────────────────────────────

/// Parse `-flag value…` pairs. A token `-<letter>…` starts a flag; everything
/// until the next flag is its value (so formulas may contain spaces). A bare
/// `-5` is a value, not a flag.
fn parse_flags(args: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut cur: Option<String> = None;
    let mut buf: Vec<String> = Vec::new();
    fn flush(out: &mut HashMap<String, String>, cur: &mut Option<String>, buf: &mut Vec<String>) {
        if let Some(k) = cur.take() {
            out.insert(k, buf.join(" "));
        }
        buf.clear();
    }
    for tok in args.split_whitespace() {
        let is_flag = tok.len() >= 2
            && tok.starts_with('-')
            && tok[1..].chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false);
        if is_flag {
            flush(&mut out, &mut cur, &mut buf);
            cur = Some(tok[1..].to_ascii_lowercase());
        } else {
            buf.push(tok.to_string());
        }
    }
    flush(&mut out, &mut cur, &mut buf);
    out
}

// ── commands ────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq)]
enum StateCmd {
    Stat { character: String, category: String, name: String, value: String },
    Modify { character: String, key: String, value: Option<String>, expr: Option<String>, turns: Option<i64>, mname: Option<String> },
    List { character: String, category: Option<String> },
    Remove { character: String, key: Option<String>, category: Option<String> },
    Define { key: String, constraint: Constraint },
    Modifiers { character: String },
    Trigger { character: String, key: String, op: String, val: f64, message: String },
    TriggerClear { character: String, key: String },
    Roll { character: String, key: Option<String>, die: u32, modifier: Option<f64>, vs: Option<f64> },
    History { character: String, key: Option<String>, limit: i64 },
    Clone { from: String, to: String },
    Join { character: String },
    Leave { character: String },
    Roster,
    Help,
}

/// Parse the args after `!state` into a command.
fn parse_state_command(args: &str) -> Option<StateCmd> {
    let (verb, rest) = split_head(args);
    let f = parse_flags(rest);
    let get = |k: &str| f.get(k).cloned().filter(|s| !s.trim().is_empty());
    match verb.to_ascii_lowercase().as_str() {
        "" => None,
        "help" => Some(StateCmd::Help),
        "stat" => Some(StateCmd::Stat {
            character: get("char")?,
            category: get("c").unwrap_or_else(|| DEFAULT_CATEGORY.to_string()),
            name: get("n")?,
            value: get("v")?,
        }),
        "modify" | "mod" => {
            let character = get("char")?;
            let key = get("k")?;
            Some(StateCmd::Modify {
                character,
                key,
                value: get("v"),
                expr: get("m"),
                turns: get("t").map(|t| parse_turns(&t)).unwrap_or(Some(-1)),
                mname: get("name"),
            })
        }
        "list" => Some(StateCmd::List { character: get("char")?, category: get("c") }),
        "remove" | "delete" | "rm" => Some(StateCmd::Remove {
            character: get("char")?,
            key: get("k"),
            category: get("c"),
        }),
        "define" => Some(StateCmd::Define {
            key: get("k")?,
            constraint: Constraint {
                ctype: get("type").map(|s| s.to_ascii_lowercase()),
                min: get("min").and_then(|s| s.parse::<f64>().ok()),
                max: get("max").and_then(|s| s.parse::<f64>().ok()),
                options: get("options").map(|s| s.split('|').map(|x| x.trim().to_string()).collect()),
            },
        }),
        "modifiers" | "mods" => Some(StateCmd::Modifiers { character: get("char")? }),
        "trigger" => {
            let character = get("char")?;
            let key = get("k")?;
            if f.contains_key("clear") {
                return Some(StateCmd::TriggerClear { character, key });
            }
            Some(StateCmd::Trigger {
                character,
                key,
                op: get("op").unwrap_or_else(|| "le".to_string()),
                val: get("v")?.parse::<f64>().ok()?,
                message: get("msg")?.trim_matches('"').to_string(),
            })
        }
        "roll" => Some(StateCmd::Roll {
            character: get("char")?,
            key: get("k"),
            die: get("d").and_then(|s| s.parse::<u32>().ok()).unwrap_or(20),
            modifier: get("mod").and_then(|s| s.parse::<f64>().ok()),
            vs: get("vs").and_then(|s| s.parse::<f64>().ok()),
        }),
        "history" | "log" => Some(StateCmd::History {
            character: get("char")?,
            key: get("k"),
            limit: get("n").and_then(|s| s.parse::<i64>().ok()).unwrap_or(10).clamp(1, 50),
        }),
        "clone" | "copy" => Some(StateCmd::Clone { from: get("char")?, to: get("to")? }),
        "join" => Some(StateCmd::Join { character: get("char")? }),
        "leave" => Some(StateCmd::Leave { character: get("char")? }),
        "roster" | "party" => Some(StateCmd::Roster),
        _ => None,
    }
}

fn split_head(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    match s.split_once(char::is_whitespace) {
        Some((h, r)) => (h, r.trim_start()),
        None => (s, ""),
    }
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

fn discord_guild(raw_data: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(raw_data)
        .ok()
        .and_then(|v| v.get("guild_id").and_then(|x| x.as_str()).map(|s| s.to_string()))
        .unwrap_or_default()
}

/// The message author's handle (for the change log's `actor`).
fn discord_actor(raw_data: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(raw_data)
        .ok()
        .and_then(|v| v.get("author").and_then(|a| a.get("username")).and_then(|x| x.as_str()).map(|s| s.to_string()))
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
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

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
                "CREATE TABLE IF NOT EXISTS stats (\
                 character TEXT NOT NULL, category TEXT NOT NULL, skey TEXT NOT NULL, \
                 svalue TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (character, skey))",
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
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS modifiers (\
                 character TEXT NOT NULL, mname TEXT NOT NULL, skey TEXT NOT NULL, \
                 expr TEXT NOT NULL, turns INTEGER NOT NULL, ticked INTEGER NOT NULL DEFAULT 0, \
                 created_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (character, mname))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS triggers (\
                 character TEXT NOT NULL, skey TEXT NOT NULL, op TEXT NOT NULL, \
                 val REAL NOT NULL, message TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT 0, \
                 PRIMARY KEY (character, skey))",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS changelog (\
                 id INTEGER PRIMARY KEY, character TEXT NOT NULL, skey TEXT NOT NULL, \
                 oldv TEXT, newv TEXT, actor TEXT, source TEXT, at INTEGER NOT NULL DEFAULT 0)",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        // Adventure membership. A character with NO row is ACTIVE (default);
        // `leave` writes an active=0 row so its modifiers stop ticking.
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS characters (\
                 character TEXT PRIMARY KEY, active INTEGER NOT NULL DEFAULT 1, \
                 updated_at INTEGER NOT NULL DEFAULT 0)",
                (),
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Mark a character in/out of the adventure (DELETE + INSERT: no upsert in
    /// turso 0.1.0).
    async fn set_active(&self, character: &str, active: bool) -> Result<(), String> {
        self.conn
            .execute("DELETE FROM characters WHERE character = ?1", turso::params![vtext(character)])
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO characters (character, active, updated_at) VALUES (?1, ?2, ?3)",
                turso::params![vtext(character), vint(active as i64), vint(now_ms())],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn inactive_characters(&self) -> Result<Vec<String>, String> {
        let mut rows = self
            .conn
            .query("SELECT character FROM characters WHERE active = 0", ())
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            if let Ok(c) = r.get::<String>(0) {
                out.push(c);
            }
        }
        Ok(out)
    }

    async fn distinct_characters(&self) -> Result<Vec<String>, String> {
        let mut rows = self
            .conn
            .query("SELECT DISTINCT character FROM stats ORDER BY character ASC", ())
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            if let Ok(c) = r.get::<String>(0) {
                out.push(c);
            }
        }
        Ok(out)
    }

    async fn set_trigger(&self, character: &str, key: &str, t: &Trigger) -> Result<(), String> {
        self.conn
            .execute("DELETE FROM triggers WHERE character = ?1 AND skey = ?2", turso::params![vtext(character), vtext(key)])
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO triggers (character, skey, op, val, message, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                turso::params![vtext(character), vtext(key), vtext(&t.op), vreal(t.val), vtext(&t.message), vint(now_ms())],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn get_trigger(&self, character: &str, key: &str) -> Result<Option<Trigger>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT op, val, message FROM triggers WHERE character = ?1 AND skey = ?2",
                turso::params![vtext(character), vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = rows.next().await.map_err(|e| e.to_string())?.map(|r| Trigger {
            op: r.get::<String>(0).unwrap_or_default(),
            val: r.get::<f64>(1).unwrap_or(0.0),
            message: r.get::<String>(2).unwrap_or_default(),
        });
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    async fn remove_trigger(&self, character: &str, key: &str) -> Result<bool, String> {
        let had = self.get_trigger(character, key).await?.is_some();
        self.conn
            .execute("DELETE FROM triggers WHERE character = ?1 AND skey = ?2", turso::params![vtext(character), vtext(key)])
            .await
            .map_err(|e| e.to_string())?;
        Ok(had)
    }

    async fn log_change(&self, character: &str, key: &str, old: Option<&Value>, new: &Value, actor: &str, source: &str) {
        let _ = self
            .conn
            .execute(
                "INSERT INTO changelog (character, skey, oldv, newv, actor, source, at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                turso::params![
                    vtext(character),
                    vtext(key),
                    old.map(|v| vtext(&v.display())).unwrap_or_else(vnull),
                    vtext(&new.display()),
                    vtext(actor),
                    vtext(source),
                    vint(now_ms())
                ],
            )
            .await;
    }

    async fn history(&self, character: &str, key: Option<&str>, limit: i64) -> Result<Vec<Change>, String> {
        let (sql, params): (&str, Vec<turso::Value>) = match key {
            Some(k) => (
                "SELECT skey, oldv, newv, actor, source, at FROM changelog WHERE character = ?1 AND skey = ?2 ORDER BY id DESC LIMIT ?3",
                vec![vtext(character), vtext(k), vint(limit)],
            ),
            None => (
                "SELECT skey, oldv, newv, actor, source, at FROM changelog WHERE character = ?1 ORDER BY id DESC LIMIT ?2",
                vec![vtext(character), vint(limit)],
            ),
        };
        let mut rows = self.conn.query(sql, params).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            out.push(Change {
                key: r.get::<String>(0).unwrap_or_default(),
                old: r.get::<Option<String>>(1).ok().flatten(),
                new: r.get::<String>(2).unwrap_or_default(),
                actor: r.get::<String>(3).unwrap_or_default(),
                source: r.get::<String>(4).unwrap_or_default(),
                at: r.get::<i64>(5).unwrap_or(0),
            });
        }
        Ok(out)
    }

    async fn count_stats(&self, character: &str) -> Result<usize, String> {
        let mut rows = self
            .conn
            .query("SELECT COUNT(*) FROM stats WHERE character = ?1", turso::params![vtext(character)])
            .await
            .map_err(|e| e.to_string())?;
        let n = match rows.next().await.map_err(|e| e.to_string())? {
            Some(r) => r.get::<i64>(0).unwrap_or(0),
            None => 0,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(n as usize)
    }

    async fn stat_get(&self, character: &str, key: &str) -> Result<Option<Stat>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT category, svalue FROM stats WHERE character = ?1 AND skey = ?2",
                turso::params![vtext(character), vtext(key)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let out = match rows.next().await.map_err(|e| e.to_string())? {
            Some(r) => {
                let category = r.get::<String>(0).unwrap_or_default();
                let value = r.get::<String>(1).ok().and_then(|s| Value::from_json(&s));
                value.map(|value| Stat { category, name: key.to_string(), value })
            }
            None => None,
        };
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    async fn stat_set(&self, character: &str, s: &Stat) -> Result<(), String> {
        self.conn
            .execute(
                "DELETE FROM stats WHERE character = ?1 AND skey = ?2",
                turso::params![vtext(character), vtext(&s.name)],
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO stats (character, category, skey, svalue, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                turso::params![vtext(character), vtext(&s.category), vtext(&s.name), vtext(&s.value.to_json()), vint(now_ms())],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn stat_remove(&self, character: &str, key: &str) -> Result<bool, String> {
        let had = self.stat_get(character, key).await?.is_some();
        self.conn
            .execute("DELETE FROM stats WHERE character = ?1 AND skey = ?2", turso::params![vtext(character), vtext(key)])
            .await
            .map_err(|e| e.to_string())?;
        Ok(had)
    }

    async fn remove_category(&self, character: &str, category: &str) -> Result<usize, String> {
        let before = self.list(character, None).await?.len();
        self.conn
            .execute(
                "DELETE FROM stats WHERE character = ?1 AND category = ?2",
                turso::params![vtext(character), vtext(category)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let after = self.list(character, None).await?.len();
        Ok(before - after)
    }

    async fn list(&self, character: &str, category: Option<&str>) -> Result<Vec<Stat>, String> {
        let (sql, params): (&str, Vec<turso::Value>) = match category {
            Some(c) => (
                "SELECT category, skey, svalue FROM stats WHERE character = ?1 AND category = ?2 ORDER BY category ASC, skey ASC",
                vec![vtext(character), vtext(c)],
            ),
            None => (
                "SELECT category, skey, svalue FROM stats WHERE character = ?1 ORDER BY category ASC, skey ASC",
                vec![vtext(character)],
            ),
        };
        let mut rows = self.conn.query(sql, params).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            let category = r.get::<String>(0).unwrap_or_default();
            let name = r.get::<String>(1).unwrap_or_default();
            if let Ok(sv) = r.get::<String>(2) {
                if let Some(value) = Value::from_json(&sv) {
                    out.push(Stat { category, name, value });
                }
            }
        }
        Ok(out)
    }

    async fn set_constraint(&self, key: &str, c: &Constraint) -> Result<(), String> {
        let opts = c.options.as_ref().map(|o| serde_json::to_string(o).unwrap());
        self.conn
            .execute("DELETE FROM constraints WHERE skey = ?1", turso::params![vtext(key)])
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO constraints (skey, ctype, cmin, cmax, coptions, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
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
            .query("SELECT ctype, cmin, cmax, coptions FROM constraints WHERE skey = ?1", turso::params![vtext(key)])
            .await
            .map_err(|e| e.to_string())?;
        let out = rows.next().await.map_err(|e| e.to_string())?.map(|r| Constraint {
            ctype: r.get::<Option<String>>(0).ok().flatten(),
            min: r.get::<Option<f64>>(1).ok().flatten(),
            max: r.get::<Option<f64>>(2).ok().flatten(),
            options: r.get::<Option<String>>(3).ok().flatten().and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok()),
        });
        while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
        Ok(out)
    }

    async fn set_modifier(&self, character: &str, m: &Modifier) -> Result<(), String> {
        self.conn
            .execute(
                "DELETE FROM modifiers WHERE character = ?1 AND mname = ?2",
                turso::params![vtext(character), vtext(&m.name)],
            )
            .await
            .map_err(|e| e.to_string())?;
        self.conn
            .execute(
                "INSERT INTO modifiers (character, mname, skey, expr, turns, ticked, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                turso::params![
                    vtext(character),
                    vtext(&m.name),
                    vtext(&m.key),
                    vtext(&m.expr),
                    vint(m.turns),
                    vint(m.ticked),
                    vint(now_ms())
                ],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn remove_modifier(&self, character: &str, name: &str) -> Result<bool, String> {
        let had = self.list_modifiers(character).await?.iter().any(|m| m.name == name);
        self.conn
            .execute("DELETE FROM modifiers WHERE character = ?1 AND mname = ?2", turso::params![vtext(character), vtext(name)])
            .await
            .map_err(|e| e.to_string())?;
        Ok(had)
    }

    async fn list_modifiers(&self, character: &str) -> Result<Vec<Modifier>, String> {
        let mut rows = self
            .conn
            .query(
                "SELECT mname, skey, expr, turns, ticked FROM modifiers WHERE character = ?1 ORDER BY mname ASC",
                turso::params![vtext(character)],
            )
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            out.push(Modifier {
                name: r.get::<String>(0).unwrap_or_default(),
                key: r.get::<String>(1).unwrap_or_default(),
                expr: r.get::<String>(2).unwrap_or_default(),
                turns: r.get::<i64>(3).unwrap_or(-1),
                ticked: r.get::<i64>(4).unwrap_or(0),
            });
        }
        Ok(out)
    }

    /// Every (character, modifier) in the store — for the per-message turn tick.
    async fn all_modifiers(&self) -> Result<Vec<(String, Modifier)>, String> {
        let mut rows = self
            .conn
            .query("SELECT character, mname, skey, expr, turns, ticked FROM modifiers", ())
            .await
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(r) = rows.next().await.map_err(|e| e.to_string())? {
            let character = r.get::<String>(0).unwrap_or_default();
            out.push((
                character,
                Modifier {
                    name: r.get::<String>(1).unwrap_or_default(),
                    key: r.get::<String>(2).unwrap_or_default(),
                    expr: r.get::<String>(3).unwrap_or_default(),
                    turns: r.get::<i64>(4).unwrap_or(-1),
                    ticked: r.get::<i64>(5).unwrap_or(0),
                },
            ));
        }
        Ok(out)
    }
}

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

// ── the turn tick ───────────────────────────────────────────────────────────

/// Advance every modifier in the store by one turn: evaluate its formula
/// (`t` = this turn number; other names = the character's current stat values),
/// add the result to the stat, clamp to the constraint, and decrement/expire.
async fn tick_modifiers(store: &Store, settings: &Settings) -> Vec<String> {
    let mut fired = Vec::new();
    let mods = match store.all_modifiers().await {
        Ok(m) => m,
        Err(_) => return fired,
    };
    // Characters that LEFT the adventure are frozen: their modifiers don't tick.
    let frozen: std::collections::HashSet<String> = store.inactive_characters().await.unwrap_or_default().into_iter().collect();
    for (character, m) in mods {
        if frozen.contains(&character) {
            continue;
        }
        // Build the variable context: t + the character's numeric stats.
        let mut vars: HashMap<String, f64> = HashMap::new();
        vars.insert("t".to_string(), (m.ticked + 1) as f64);
        if let Ok(stats) = store.list(&character, None).await {
            for s in stats {
                if let Some(n) = s.value.as_f64() {
                    vars.insert(s.name.to_ascii_lowercase(), n);
                }
            }
        }
        let delta = match eval_formula(&m.expr, &vars) {
            Ok(d) => d,
            Err(e) => {
                warn!("modifier `{}` on `{character}`: bad formula `{}`: {e}", m.name, m.expr);
                continue;
            }
        };

        let con = {
            let op = settings.schema.get(&m.key).cloned().unwrap_or_default();
            let user = store.get_constraint(&m.key).await.unwrap_or(None).unwrap_or_default();
            op.merged_with_user(&user)
        };
        let cur = store.stat_get(&character, &m.key).await.unwrap_or(None);
        let (base, was_int) = match &cur {
            Some(s) => match &s.value {
                Value::Int(i) => (*i as f64, true),
                Value::Float(f) => (*f, false),
                _ => continue, // non-numeric: cannot modify
            },
            None => (0.0, delta.fract() == 0.0),
        };
        let mut nv = base + delta;
        if let Some(min) = con.min {
            nv = nv.max(min);
        }
        if let Some(max) = con.max {
            nv = nv.min(max);
        }
        let value = if was_int && delta.fract() == 0.0 { Value::Int(nv as i64) } else { Value::Float(nv) };
        fired.extend(write_stat(store, &character, &m.key, value, "", "modifier", None).await);

        // Decrement / expire.
        let next = Modifier { ticked: m.ticked + 1, ..m.clone() };
        if m.turns > 0 {
            let t = m.turns - 1;
            if t == 0 {
                let _ = store.remove_modifier(&character, &m.name).await;
            } else {
                let _ = store.set_modifier(&character, &Modifier { turns: t, ..next }).await;
            }
        } else {
            let _ = store.set_modifier(&character, &next).await;
        }
    }
    fired
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
        payload: Some(EnginePayload::Commands(Commands { commands: vec![], alert_on_unknown_command: false })),
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

/// Resolve the effective constraint for a key (operator schema wins).
async fn effective_constraint(store: &Store, settings: &Settings, key: &str) -> Constraint {
    let op = settings.schema.get(key).cloned().unwrap_or_default();
    let user = store.get_constraint(key).await.unwrap_or(None).unwrap_or_default();
    op.merged_with_user(&user)
}

/// Write a stat value, record it in the change log, and fire any threshold
/// trigger the write CROSSES (the old value did not satisfy it, the new does).
/// Returns the trigger messages to announce.
async fn write_stat(
    store: &Store,
    character: &str,
    key: &str,
    new: Value,
    actor: &str,
    source: &str,
    category: Option<&str>,
) -> Vec<String> {
    let old = store.stat_get(character, key).await.unwrap_or(None);
    let category = category
        .map(|s| s.to_string())
        .or_else(|| old.as_ref().map(|s| s.category.clone()))
        .unwrap_or_else(|| DEFAULT_CATEGORY.to_string());
    let _ = store
        .stat_set(character, &Stat { category, name: key.to_string(), value: new.clone() })
        .await;
    store.log_change(character, key, old.as_ref().map(|s| &s.value), &new, actor, source).await;
    if let Some(t) = store.get_trigger(character, key).await.unwrap_or(None) {
        let was = old.as_ref().and_then(|s| s.value.as_f64());
        if t.satisfied(new.as_f64()) && !t.satisfied(was) {
            return vec![t.message];
        }
    }
    Vec::new()
}

async fn handle_command(
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    settings: &Settings,
    guild: &str,
    cmd: StateCmd,
) -> Vec<ContainerForEngine> {
    let mut out = Vec::new();
    macro_rules! push {
        ($m:expr) => {
            out.push(reply_container(id, chat, $m))
        };
    }
    let store = match stores.guild(guild).await {
        Ok(s) => s,
        Err(e) => {
            push!(format!("storage error: {e}"));
            return out;
        }
    };
    let actor = discord_actor(&chat.raw_data);

    match cmd {
        StateCmd::Help => {
            let f = &settings.command_flag;
            push!(format!(
                "**stats**: `{f}state stat -char <c> [-c <cat>] -n <name> -v <value>` · \
                 `{f}state modify -char <c> -k <name> -v <value>` · \
                 `{f}state modify -char <c> -k <name> -m <formula> [-t <turns>] [-name <m>]` · \
                 `{f}state list -char <c> [-c <cat>]` · \
                 `{f}state remove -char <c> -k <name>` | `-c <cat>` · \
                 `{f}state define -k <name> [-type ..] [-min ..] [-max ..] [-options a|b]`"
            ));
            push!(format!(
                "**effects & checks**: `{f}state modifiers -char <c>` · \
                 `{f}state trigger -char <c> -k <name> [-op le] -v <n> -msg <text>` · \
                 `{f}state roll -char <c> [-k <stat>] [-d <die>] [-vs <n>]` · \
                 `{f}state history -char <c> [-k <name>]`"
            ));
            push!(format!(
                "**characters**: `{f}state clone -char <src> -to <dst>` · \
                 `{f}state join -char <c>` / `{f}state leave -char <c>` · `{f}state roster`"
            ));
        }
        StateCmd::Stat { character, category, name, value } => {
            if name.len() > MAX_NAME_LEN {
                push!(format!("name too long (max {MAX_NAME_LEN})."));
                return out;
            }
            if value.len() > MAX_VALUE_LEN {
                push!(format!("value too long (max {MAX_VALUE_LEN})."));
                return out;
            }
            let con = effective_constraint(store, settings, &name).await;
            let v = match con.ctype.as_deref() {
                Some(t) => match coerce(&value, t) {
                    Ok(v) => v,
                    Err(e) => {
                        push!(format!("`{name}`: {e}."));
                        return out;
                    }
                },
                None => infer_value(&value),
            };
            if let Err(e) = con.validate(&v) {
                push!(format!("`{name}`: {e}."));
                return out;
            }
            if store.stat_get(&character, &name).await.unwrap_or(None).is_none()
                && store.count_stats(&character).await.unwrap_or(0) >= MAX_STATS_PER_CHAR
            {
                push!(format!("`{character}` already has {MAX_STATS_PER_CHAR} stats (the cap)."));
                return out;
            }
            push!(format!("`{character}` [{category}] `{name}` = {}.", v.display()));
            for m in write_stat(store, &character, &name, v, &actor, "stat", Some(&category)).await {
                push!(m);
            }
        }
        StateCmd::Modify { character, key, value, expr, turns, mname } => {
            match (value, expr) {
                // Set the value directly.
                (Some(v), None) => {
                    let con = effective_constraint(store, settings, &key).await;
                    let val = match con.ctype.as_deref() {
                        Some(t) => match coerce(&v, t) {
                            Ok(x) => x,
                            Err(e) => {
                                push!(format!("`{key}`: {e}."));
                                return out;
                            }
                        },
                        None => infer_value(&v),
                    };
                    if let Err(e) = con.validate(&val) {
                        push!(format!("`{key}`: {e}."));
                        return out;
                    }
                    push!(format!("`{character}` `{key}` = {}.", val.display()));
                    for m in write_stat(store, &character, &key, val, &actor, "modify", None).await {
                        push!(m);
                    }
                }
                // Add/replace a formula modifier.
                (None, Some(formula)) => {
                    // Validate the formula parses.
                    if let Err(e) = eval_formula(&formula, &HashMap::new()) {
                        // Unknown names are fine (resolved at tick); only syntax matters.
                        if !e.starts_with("unknown name") {
                            push!(format!("bad formula `{formula}`: {e}."));
                            return out;
                        }
                    }
                    let name = mname.unwrap_or_else(|| key.clone());
                    let m = Modifier { name: name.clone(), key: key.clone(), expr: formula.clone(), turns: turns.unwrap_or(-1), ticked: 0 };
                    match store.set_modifier(&character, &m).await {
                        Ok(()) => {
                            let dur = if m.turns < 0 { "indefinitely".to_string() } else { format!("for {} turn(s)", m.turns) };
                            push!(format!("Modifier `{name}`: `{key}` += `{formula}` per turn {dur} on `{character}`."));
                        }
                        Err(e) => push!(format!("storage error: {e}")),
                    }
                }
                _ => push!("pass either `-v <value>` or `-m <formula>`.".to_string()),
            }
        }
        StateCmd::List { character, category } => match store.list(&character, category.as_deref()).await {
            Ok(items) if items.is_empty() => push!(format!("`{character}` has no stats{}.",
                category.map(|c| format!(" in [{c}]")).unwrap_or_default())),
            Ok(items) => {
                let mut by_cat: Vec<(String, Vec<String>)> = Vec::new();
                for s in items {
                    let entry = format!("`{}`={}", s.name, s.value.display());
                    match by_cat.iter_mut().find(|(c, _)| *c == s.category) {
                        Some((_, v)) => v.push(entry),
                        None => by_cat.push((s.category, vec![entry])),
                    }
                }
                let body = by_cat.iter().map(|(c, v)| format!("[{c}] {}", v.join(", "))).collect::<Vec<_>>().join(" · ");
                push!(format!("`{character}`: {body}"));
            }
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Remove { character, key, category } => match (key, category) {
            (Some(k), None) => match store.stat_remove(&character, &k).await {
                Ok(true) => push!(format!("Removed `{k}` from `{character}`.")),
                Ok(false) => push!(format!("`{character}` has no stat `{k}`.")),
                Err(e) => push!(format!("storage error: {e}")),
            },
            (None, Some(c)) => match store.remove_category(&character, &c).await {
                Ok(0) => push!(format!("`{character}` has no stats in [{c}].")),
                Ok(n) => push!(format!("Removed {n} stat(s) in [{c}] from `{character}`.")),
                Err(e) => push!(format!("storage error: {e}")),
            },
            _ => push!("pass either `-k <name>` or `-c <category>`.".to_string()),
        },
        StateCmd::Define { key, constraint } => {
            if constraint.is_empty() {
                push!("Nothing to define — pass `-type`/`-min`/`-max`/`-options`.".to_string());
                return out;
            }
            if let Some(t) = &constraint.ctype {
                if !["int", "float", "string", "bool", "options"].contains(&t.as_str()) {
                    push!(format!("unknown type `{t}` (int|float|string|bool|options)."));
                    return out;
                }
            }
            match store.set_constraint(&key, &constraint).await {
                Ok(()) => push!(format!("Constraint on `{key}`: {}.", constraint.describe())),
                Err(e) => push!(format!("storage error: {e}")),
            }
        }
        StateCmd::Modifiers { character } => match store.list_modifiers(&character).await {
            Ok(ms) if ms.is_empty() => push!(format!("`{character}` has no modifiers.")),
            Ok(ms) => push!(format!(
                "`{character}`: {}",
                ms.iter().map(describe_modifier).collect::<Vec<_>>().join(", ")
            )),
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Trigger { character, key, op, val, message } => {
            let t = Trigger { op: op.clone(), val, message: message.clone() };
            match store.set_trigger(&character, &key, &t).await {
                Ok(()) => push!(format!("Trigger on `{character}` `{key}`: when {op} {val} → \"{message}\".")),
                Err(e) => push!(format!("storage error: {e}")),
            }
        }
        StateCmd::TriggerClear { character, key } => match store.remove_trigger(&character, &key).await {
            Ok(true) => push!(format!("Removed trigger on `{character}` `{key}`.")),
            Ok(false) => push!(format!("`{character}` has no trigger on `{key}`.")),
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Roll { character, key, die, modifier, vs } => {
            let stat_mod = match &key {
                Some(k) => store.stat_get(&character, k).await.unwrap_or(None).and_then(|s| s.value.as_f64()),
                None => None,
            };
            let bonus = stat_mod.unwrap_or(0.0) + modifier.unwrap_or(0.0);
            let roll = roll_die(die);
            let total = roll as f64 + bonus;
            let label = key.map(|k| format!(" + `{k}`")).unwrap_or_default();
            match vs {
                Some(target) => {
                    let ok = total >= target;
                    push!(format!(
                        "🎲 `{character}` rolled d{die}: {roll}{label} ({bonus:+.0}) = {total:.0} vs {target:.0} → {}",
                        if ok { "SUCCESS" } else { "FAILURE" }
                    ));
                }
                None => push!(format!("🎲 `{character}` rolled d{die}: {roll}{label} ({bonus:+.0}) = {total:.0}")),
            }
        }
        StateCmd::History { character, key, limit } => match store.history(&character, key.as_deref(), limit).await {
            Ok(h) if h.is_empty() => push!(format!("`{character}` has no history.")),
            Ok(h) => {
                let body = h
                    .iter()
                    .map(|c| {
                        let by = if c.actor.is_empty() { c.source.clone() } else { c.actor.clone() };
                        format!("`{}`: {} → {} ({by})", c.key, c.old.clone().unwrap_or_else(|| "∅".into()), c.new)
                    })
                    .collect::<Vec<_>>()
                    .join(" · ");
                push!(format!("`{character}` (last {}): {body}", h.len()));
            }
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Clone { from, to } => {
            if from == to {
                push!("source and destination are the same character.".to_string());
                return out;
            }
            let src = store.list(&from, None).await.unwrap_or_default();
            if src.is_empty() {
                push!(format!("`{from}` has no stats to clone."));
                return out;
            }
            let n = src.len();
            for s in src {
                // A clone is a silent copy — do not fire the destination's triggers.
                let _ = write_stat(store, &to, &s.name, s.value.clone(), &actor, "clone", Some(&s.category)).await;
            }
            let _ = store.set_active(&to, true).await;
            push!(format!("Cloned {n} stat(s) from `{from}` to `{to}`."));
        }
        StateCmd::Join { character } => match store.set_active(&character, true).await {
            Ok(()) => push!(format!("`{character}` joined the adventure — its modifiers tick again.")),
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Leave { character } => match store.set_active(&character, false).await {
            Ok(()) => push!(format!("`{character}` left the adventure — frozen until it rejoins.")),
            Err(e) => push!(format!("storage error: {e}")),
        },
        StateCmd::Roster => {
            let chars = store.distinct_characters().await.unwrap_or_default();
            let frozen: std::collections::HashSet<String> = store.inactive_characters().await.unwrap_or_default().into_iter().collect();
            if chars.is_empty() {
                push!("No characters yet.".to_string());
            } else {
                let body = chars
                    .iter()
                    .map(|c| if frozen.contains(c) { format!("`{c}` (frozen)") } else { format!("`{c}`") })
                    .collect::<Vec<_>>()
                    .join(", ");
                push!(format!("Adventure roster: {body}"));
            }
        }
    }
    out
}

/// Handle one message: tick modifiers one turn (an ordinary message), then
/// handle a `!state` command if the message is one.
async fn handle_message(
    id: &EngineIdentity,
    chat: &ChatMessage,
    stores: &mut Stores,
    settings: &Settings,
) -> Vec<ContainerForEngine> {
    let guild = discord_guild(&chat.raw_data);
    let guild_key = if guild.is_empty() { DM_DB.to_string() } else { guild };

    // A `!state` command does NOT advance a turn.
    if let Some(args) = parse_state_args(&chat.raw_message, &settings.command_flag, STATE_COMMAND) {
        let Some(cmd) = parse_state_command(&args) else {
            return Vec::new();
        };
        return handle_command(id, chat, stores, settings, &guild_key, cmd).await;
    }

    // Otherwise this ordinary message is ONE TURN: advance every modifier.
    let mut out = Vec::new();
    if let Ok(store) = stores.guild(&guild_key).await {
        for m in tick_modifiers(store, settings).await {
            out.push(reply_container(id, chat, m));
        }
    }
    out
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
                                payload: Some(EnginePayload::MessageAck(MessageAck { message_uuid7: process.message_uuid7.clone() })),
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
    fn infers_and_coerces_values() {
        assert_eq!(infer_value("42"), Value::Int(42));
        assert_eq!(infer_value("4.2"), Value::Float(4.2));
        assert_eq!(infer_value("true"), Value::Bool(true));
        assert_eq!(infer_value("hi"), Value::Str("hi".into()));
        assert_eq!(coerce("42", "string").unwrap(), Value::Str("42".into()));
        assert!(coerce("x", "int").is_err());
    }

    #[test]
    fn validates_constraints() {
        let c = Constraint { ctype: Some("int".into()), min: Some(0.0), max: Some(20.0), options: None };
        assert!(c.validate(&Value::Int(20)).is_ok());
        assert!(c.validate(&Value::Int(21)).is_err());
        let opts = Constraint { ctype: Some("options".into()), options: Some(vec!["a".into(), "b".into()]), ..Default::default() };
        assert!(opts.validate(&Value::Str("a".into())).is_ok());
        assert!(opts.validate(&Value::Str("z".into())).is_err());
    }

    #[test]
    fn evaluates_formulas() {
        let mut vars = HashMap::new();
        vars.insert("t".to_string(), 3.0);
        vars.insert("hp".to_string(), 40.0);
        assert_eq!(eval_formula("t*2", &vars).unwrap(), 6.0);
        assert_eq!(eval_formula("hp/10 + 1", &vars).unwrap(), 5.0);
        assert_eq!(eval_formula("-t + 10", &vars).unwrap(), 7.0);
        assert_eq!(eval_formula("(t+1)*2", &vars).unwrap(), 8.0);
        assert!(eval_formula("t/0", &vars).is_err());
        assert!(eval_formula("hp +", &vars).is_err());
        assert!(eval_formula("(t", &vars).is_err());
        assert!(eval_formula("nope", &vars).is_err());
    }

    #[test]
    fn parses_flags() {
        let f = parse_flags("-char alice -c health -n hp -v 100");
        assert_eq!(f.get("char").map(String::as_str), Some("alice"));
        assert_eq!(f.get("c").map(String::as_str), Some("health"));
        assert_eq!(f.get("n").map(String::as_str), Some("hp"));
        assert_eq!(f.get("v").map(String::as_str), Some("100"));
        // a multi-token formula value
        let f = parse_flags("-char alice -k hp -m t * 2 -t 7");
        assert_eq!(f.get("m").map(String::as_str), Some("t * 2"));
        assert_eq!(f.get("t").map(String::as_str), Some("7"));
        // a bare negative number is a value, not a flag
        let f = parse_flags("-v -5");
        assert_eq!(f.get("v").map(String::as_str), Some("-5"));
    }

    #[test]
    fn parses_commands() {
        assert_eq!(
            parse_state_command("stat -char alice -c health -n hp -v 100"),
            Some(StateCmd::Stat { character: "alice".into(), category: "health".into(), name: "hp".into(), value: "100".into() })
        );
        // category defaults to general
        assert_eq!(
            parse_state_command("stat -char alice -n hp -v 100"),
            Some(StateCmd::Stat { character: "alice".into(), category: DEFAULT_CATEGORY.into(), name: "hp".into(), value: "100".into() })
        );
        assert_eq!(
            parse_state_command("modify -char alice -k hp -v 90"),
            Some(StateCmd::Modify { character: "alice".into(), key: "hp".into(), value: Some("90".into()), expr: None, turns: Some(-1), mname: None })
        );
        assert_eq!(
            parse_state_command("modify -char alice -k hp -m t*2 -t 7 -name regen"),
            Some(StateCmd::Modify { character: "alice".into(), key: "hp".into(), value: None, expr: Some("t*2".into()), turns: Some(7), mname: Some("regen".into()) })
        );
        assert_eq!(
            parse_state_command("list -char alice -c health"),
            Some(StateCmd::List { character: "alice".into(), category: Some("health".into()) })
        );
        assert_eq!(
            parse_state_command("remove -char alice -c health"),
            Some(StateCmd::Remove { character: "alice".into(), key: None, category: Some("health".into()) })
        );
        assert_eq!(parse_state_command("help"), Some(StateCmd::Help));
        // missing -char → not a command
        assert_eq!(parse_state_command("stat -n hp -v 1"), None);
        assert_eq!(parse_state_command("bogus"), None);
    }

    #[test]
    fn parses_turns_non_positive_is_indefinite() {
        assert_eq!(parse_turns("7"), Some(7));
        assert_eq!(parse_turns("x7"), Some(7));
        for t in ["inf", "infinite", "forever", "-1", "x-1", "0"] {
            assert_eq!(parse_turns(t), Some(-1), "{t}");
        }
    }

    // ── end-to-end (a fake character sheet) ─────────────────────────────────

    fn discord_msg(guild: &str, text: &str) -> ChatMessage {
        let raw = serde_json::json!({
            "id": "m1", "guild_id": guild, "channel_id": "c1",
            "content": text, "author": {"id": "1", "username": "alice"}, "mentions": []
        })
        .to_string();
        ChatMessage {
            platform: "discord".into(),
            raw_data: raw.into_bytes(),
            raw_message: text.into(),
            user_uuid7: "u1".into(),
            command: None,
            user_data: Some(UserData { username: "alice".into(), ..Default::default() }),
            channel_id: "c1".into(),
        }
    }

    fn test_settings() -> Settings {
        Settings { command_flag: "!".into(), db_dir: String::new(), reconnect_base_secs: 1, reconnect_max_secs: 30, schema: HashMap::new() }
    }

    fn test_id() -> EngineIdentity {
        EngineIdentity { auth: "t".into(), instance: "i".into(), module: "state".into() }
    }

    fn replies(v: &[ContainerForEngine]) -> Vec<String> {
        v.iter()
            .filter_map(|c| match &c.payload {
                Some(EnginePayload::SendToPlatforms(s)) => Some(s.msg.clone()),
                _ => None,
            })
            .collect()
    }

    async fn say(stores: &mut Stores, settings: &Settings, text: &str) -> Vec<ContainerForEngine> {
        handle_message(&test_id(), &discord_msg("g1", text), stores, settings).await
    }

    #[tokio::test]
    async fn character_stat_flow() {
        let dir = std::env::temp_dir().join(format!("cockatiel-state-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let mut set = test_settings();
        set.schema.insert("hp".into(), Constraint { ctype: Some("int".into()), min: Some(0.0), max: Some(100.0), options: None });
        let guild = "g1";

        // define two stats (one categorised)
        let out = say(&mut stores, &set, "!state stat -char alice -c vitals -n hp -v 50").await;
        assert!(replies(&out)[0].contains("[vitals] `hp` = 50"), "{:?}", replies(&out));
        say(&mut stores, &set, "!state stat -char alice -n mana -v 30").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(50));

        // list groups by category
        let out = say(&mut stores, &set, "!state list -char alice").await;
        assert!(replies(&out)[0].contains("[vitals] `hp`=50") && replies(&out)[0].contains("[general] `mana`=30"), "{:?}", replies(&out));

        // modify a value
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -v 40").await;
        assert!(replies(&out)[0].contains("`hp` = 40"));

        // a formula modifier: hp += t*2 for 3 turns
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -m t*2 -t 3 -name regen").await;
        assert!(replies(&out)[0].contains("Modifier `regen`"), "{:?}", replies(&out));

        // each ordinary message = one turn: 40 -> 42 -> 46 -> 52, then expires
        say(&mut stores, &set, "hello").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(42));
        say(&mut stores, &set, "again").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(46));
        say(&mut stores, &set, "again").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(52));
        assert!(stores.guild(guild).await.unwrap().list_modifiers("alice").await.unwrap().is_empty(), "expired");

        // clamping: a huge modifier is capped by max=100
        say(&mut stores, &set, "!state modify -char alice -k hp -m 999 -t 1").await;
        say(&mut stores, &set, "go").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(100));

        // a formula referencing another stat: mana += hp/10 per turn
        say(&mut stores, &set, "!state modify -char alice -k mana -m hp/10 -t 1").await;
        say(&mut stores, &set, "tick").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "mana").await.unwrap().unwrap().value, Value::Int(40)); // 30 + 100/10

        // bulk remove a category
        say(&mut stores, &set, "!state stat -char alice -c junk -n a -v 1").await;
        say(&mut stores, &set, "!state stat -char alice -c junk -n b -v 2").await;
        let out = say(&mut stores, &set, "!state remove -char alice -c junk").await;
        assert!(replies(&out)[0].contains("Removed 2 stat(s) in [junk]"), "{:?}", replies(&out));

        // constraints reject out-of-range / wrong type
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -v 999").await;
        assert!(replies(&out)[0].contains("must be <= 100"), "{:?}", replies(&out));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn define_constraint_and_list_modifiers() {
        let dir = std::env::temp_dir().join(format!("cockatiel-state-def-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let set = test_settings();

        // a user-defined constraint is enforced
        let out = say(&mut stores, &set, "!state define -k level -type int -min 1 -max 10").await;
        assert!(replies(&out)[0].contains("Constraint on `level`"), "{:?}", replies(&out));
        let out = say(&mut stores, &set, "!state stat -char alice -n level -v 99").await;
        assert!(replies(&out)[0].contains("must be <= 10"), "{:?}", replies(&out));
        say(&mut stores, &set, "!state stat -char alice -n level -v 5").await;

        // modifiers list shows the effect + its current turn
        say(&mut stores, &set, "!state modify -char alice -k level -m 1 -t 3 -name up").await;
        let out = say(&mut stores, &set, "!state modifiers -char alice").await;
        assert!(replies(&out)[0].contains("`up`"), "{:?}", replies(&out));
        assert!(replies(&out)[0].contains("(3 turn(s))"), "{:?}", replies(&out));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn triggers_rolls_and_history() {
        let dir = std::env::temp_dir().join(format!("cockatiel-state-trh-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let set = test_settings();

        say(&mut stores, &set, "!state stat -char alice -n hp -v 10").await;
        let out = say(&mut stores, &set, "!state trigger -char alice -k hp -op le -v 0 -msg fallen").await;
        assert!(replies(&out)[0].contains("Trigger on `alice` `hp`"), "{:?}", replies(&out));

        // still above the threshold → no announcement
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -v 5").await;
        assert!(replies(&out).iter().all(|m| !m.contains("fallen")), "{:?}", replies(&out));

        // crossing into `<= 0` → fires
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -v 0").await;
        assert!(replies(&out).iter().any(|m| m.contains("fallen")), "{:?}", replies(&out));

        // already satisfying → no re-fire (only CROSSINGS announce)
        let out = say(&mut stores, &set, "!state modify -char alice -k hp -v 0").await;
        assert!(replies(&out).iter().all(|m| !m.contains("fallen")), "{:?}", replies(&out));

        // a trigger fired from a MODIFIER tick: hp 5 -> 0 crosses
        say(&mut stores, &set, "!state modify -char alice -k hp -v 5").await;
        say(&mut stores, &set, "!state modify -char alice -k hp -m -5 -t 1").await;
        let out = say(&mut stores, &set, "an ordinary message").await;
        assert!(replies(&out).iter().any(|m| m.contains("fallen")), "{:?}", replies(&out));

        // randomness in a formula: `d6` and `rand(1,6)` both land in range
        for _ in 0..50 {
            let r = eval_formula("d6", &HashMap::new()).unwrap();
            assert!((1.0..=6.0).contains(&r), "d6 = {r}");
            let r = eval_formula("rand(1,6)", &HashMap::new()).unwrap();
            assert!((1.0..=6.0).contains(&r), "rand = {r}");
        }

        // a roll/check
        let out = say(&mut stores, &set, "!state roll -char alice -k hp -d 20 -vs 5").await;
        assert!(replies(&out)[0].contains("rolled d20"), "{:?}", replies(&out));
        assert!(replies(&out)[0].contains("SUCCESS") || replies(&out)[0].contains("FAILURE"));

        // change log records who changed what
        let out = say(&mut stores, &set, "!state history -char alice -k hp").await;
        assert!(replies(&out)[0].contains("`hp`"), "{:?}", replies(&out));
        assert!(replies(&out)[0].contains("(alice)"), "actor recorded: {:?}", replies(&out));
        assert!(replies(&out)[0].contains("(modifier)"), "tick source recorded: {:?}", replies(&out));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn clone_and_adventure_membership() {
        let dir = std::env::temp_dir().join(format!("cockatiel-state-clone-{}", uuid::Uuid::now_v7()));
        let mut stores = Stores::new(dir.to_string_lossy().to_string());
        let set = test_settings();
        let guild = "g1";

        // a source character with two stats (one categorised)
        say(&mut stores, &set, "!state stat -char alice -c vitals -n hp -v 50").await;
        say(&mut stores, &set, "!state stat -char alice -n mana -v 30").await;

        // clone alice -> bob: a faithful, non-destructive copy of the sheet
        let out = say(&mut stores, &set, "!state clone -char alice -to bob").await;
        assert!(replies(&out)[0].contains("Cloned 2 stat(s)"), "{:?}", replies(&out));
        let bob_hp = stores.guild(guild).await.unwrap().stat_get("bob", "hp").await.unwrap().unwrap();
        assert_eq!(bob_hp.value, Value::Int(50));
        assert_eq!(bob_hp.category, "vitals");

        // a modifier on each; both tick while active
        say(&mut stores, &set, "!state modify -char alice -k hp -m 1 -t 99 -name a").await;
        say(&mut stores, &set, "!state modify -char bob -k hp -m 1 -t 99 -name b").await;
        say(&mut stores, &set, "tick").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(51));
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("bob", "hp").await.unwrap().unwrap().value, Value::Int(51));

        // bob leaves → frozen; alice keeps ticking
        let out = say(&mut stores, &set, "!state leave -char bob").await;
        assert!(replies(&out)[0].contains("frozen"), "{:?}", replies(&out));
        say(&mut stores, &set, "tick").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("alice", "hp").await.unwrap().unwrap().value, Value::Int(52));
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("bob", "hp").await.unwrap().unwrap().value, Value::Int(51), "bob is frozen");

        // roster marks bob frozen
        let out = say(&mut stores, &set, "!state roster").await;
        assert!(replies(&out)[0].contains("`bob` (frozen)"), "{:?}", replies(&out));
        assert!(replies(&out)[0].contains("`alice`"), "{:?}", replies(&out));

        // bob rejoins → ticks again
        say(&mut stores, &set, "!state join -char bob").await;
        say(&mut stores, &set, "tick").await;
        assert_eq!(stores.guild(guild).await.unwrap().stat_get("bob", "hp").await.unwrap().unwrap().value, Value::Int(52));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
