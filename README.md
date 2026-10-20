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
| `!state trigger -char <c> -k <name> [-op le\|ge\|lt\|gt\|eq\|ne] -v <n> -msg <text>` | announce when a stat crosses a bound |
| `!state trigger -char <c> -k <name> -clear` | remove a trigger |
| `!state roll -char <c> [-k <stat>] [-d <die>] [-mod <n>] [-vs <target>]` | roll a check |
| `!state history -char <c> [-k <name>] [-n <limit>]` | recent changes (who/what/when) |
| `!state clone -char <src> -to <dst>` | duplicate a character's sheet |
| `!state join -char <c>` / `!state leave -char <c>` | enter / leave the adventure |
| `!state roster` | list characters (frozen ones marked) |
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

Formulas also support **dice and functions**: `d20` / `d6` (a die roll), and
`rand(a,b)`, `min(a,b)`, `max(a,b)`, `floor(x)`, `ceil(x)`, `round(x)`, `abs(x)`,
`d(n)`. So `-m "d6 + 1"` is a random per-turn effect.

## Triggers, rolls & history

- **Triggers** announce a message when a stat **crosses** a bound (the old value
  didn't satisfy it, the new one does) — e.g.
  `!state trigger -char alice -k hp -op le -v 0 -msg "alice has fallen!"`. They
  fire from both direct edits and modifier ticks.
- **Rolls/checks**: `!state roll -char alice -k hp -d 20 -vs 15` rolls `d20` plus
  the stat (and/or `-mod`), reporting SUCCESS/FAILURE against the target.
- **History**: every stat change is logged (key, old → new, who, when) —
  `!state history -char alice [-k hp]`.

## Clone & the adventure

- **Clone** copies a character's whole sheet (stats + categories) onto another
  character, so you can spin up a copy to experiment with without risking the
  original: `!state clone -char alice -to alice_test`.
- **Adventure membership.** A turn is one ordinary message, and every *active*
  character's modifiers tick on it. `!state leave -char alice` **freezes** a
  character (its modifiers stop ticking) while the RP keeps going for everyone
  else; `!state join -char alice` brings it back. A character is active by
  default; `!state roster` lists everyone and marks the frozen ones.

## Storage

One `turso` (SQLite) database per **server** (`<db_dir>/<guild_id>.db`) holding
the `stats`, `constraints`, and `modifiers` tables. DB files gitignored.

## Config (`module_specific`)

- `command_flag` — command sigil (default `!`).
- `db_dir` — per-server DB directory (default `~/.cockatiel/state`).
- `schema` — optional operator constraints map (see above).
- `reconnect_base_secs` / `reconnect_max_secs` — reconnect backoff.
