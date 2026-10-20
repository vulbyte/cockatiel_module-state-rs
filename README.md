# cockatiel_module-state-rs

A per-user **typed key/value state** store for Cockatiel — JSON-like properties
attached to a chat user, with optional **constraints**.

## Commands

| Command | Effect |
|---|---|
| `!state set <key>=<value>` | set a property on **you** |
| `!state set @user <key>=<value>` | set a property on **someone else** (anyone may) |
| `!state get [@user] <key>` | read a property |
| `!state list [@user]` | list keys (and values) |
| `!state remove [@user] <key>` | delete a property |
| `!state define <key> [type=…] [min=…] [max=…] [options=a\|b\|c]` | constrain a key |
| `!state help` | usage |

## Values & constraints

Values are typed: **int**, **float**, **string**, **bool**, or **options** (a
string restricted to a set). Without a constraint the type is inferred from the
text (`42` → int, `4.2` → float, `true`/`false` → bool, otherwise string).

A key's constraint has three **optional** parts:

- `type` — `int` | `float` | `string` | `bool` | `options`
- `min` / `max` — numeric bounds (for int/float)
- `options` — an array of allowed strings (for `type=options`)

Constraints come from **two** places, merged (operator wins):

1. The **operator schema** in `config.json` → `module_specific.schema`
   (`{"score": {"type":"int","min":0,"max":100}}`).
2. **User-defined** constraints via `!state define` (stored per server).

A value that violates a constraint is rejected with a clear message.

## Storage

One `turso` (SQLite) database per **server** (`<db_dir>/<guild_id>.db`) holding
the `state` (subject, key, value) and `constraints` tables. Subjects are keyed by
platform identity (the stable platform user id). UUID7 keys; DB files gitignored.

## Config (`module_specific`)

- `command_flag` — command sigil (default `!`).
- `db_dir` — per-server DB directory (default `~/.cockatiel/state`).
- `schema` — optional operator constraints map (see above).
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff.
