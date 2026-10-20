# cockatiel_module-state-rs

Per-**character** stats for Cockatiel — each stat has a **category**, a **name**,
and a typed **value**, with optional **constraints** and **formula-driven
per-turn modifiers**.

A stat belongs to a character (an RP character name, e.g. `alice`). **Anyone may
read or edit any character's stats.**

## Commands (explicit flags; `-char` names the target character)

| Command | Effect |
|---|---|
| `!state stat -char <c> [-c <category>] -n <name> -v <value>` | create/update a stat |
| `!state modify -char <c> -k <name> -v <value>` | set a stat's value |
| `!state modify -char <c> -k <name> -m <formula> [-t <turns>] [-name <mname>]` | add/replace a modifier |
| `!state list -char <c> [-c <category>]` | list stats (grouped by category) |
| `!state remove -char <c> -k <name>` | remove one stat |
| `!state remove -char <c> -c <category>` | remove a whole category |
| `!state modifiers -char <c>` | list active modifiers |
| `!state define -k <name> [-type …] [-min …] [-max …] [-options a\|b\|c]` | constrain a key |
| `!state help` | usage |

`-c` (stat) is the **category**; `-k` (modify/remove/define) is the stat **key**;
`-m` is the **formula**; `-t` is the turn budget.

## Values & constraints

Values are typed: **int / float / string / bool / options** (a string restricted
to a set). Without a constraint the type is inferred from the text (`42` → int,
`4.2` → float, `true` → bool, else string).

A constraint has three **optional** parts — `type`, `min`/`max`, `options` — from
two sources, merged (**operator wins**):

1. the **operator schema** in `config.json` → `module_specific.schema`
   (`{"hp": {"type":"int","min":0,"max":100}}`), and
2. **`!state define`** (stored per server).

## Modifiers (formula status effects)

A modifier changes its stat by a **formula** every **turn** (a turn = one
ordinary message; `!state` commands don't count). In the formula, `t` is the
modifier's turn number and **any stat name** is a variable (its current value):

```
!state modify -char alice -k hp -m t*2              # +2, +4, +6 … each turn
!state modify -char alice -k hp -m "hp/10 + 1"      # references another stat
!state modify -char alice -k hp -m 5 -t 7 -name regen   # +5/turn, 7 turns
```

The turn budget `-t <n>` defaults to (and is forced to) **indefinite** when
omitted, `inf`, or non-positive. `-name` names the modifier (defaults to the
key); re-adding a name replaces it. The result is clamped to the key's
constraint, and a non-numeric stat is skipped.

## Storage

One `turso` (SQLite) database per **server** (`<db_dir>/<guild_id>.db`) holding
the `stats`, `constraints`, and `modifiers` tables. DB files gitignored.

## Config (`module_specific`)

- `command_flag` — command sigil (default `!`).
- `db_dir` — per-server DB directory (default `~/.cockatiel/state`).
- `schema` — optional operator constraints map (see above).
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff.
