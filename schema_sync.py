"""Schema reconciliation for eTrak MySQL/MariaDB databases.

WHY THIS EXISTS
---------------
The previous startup path could not update an existing database, which is why
features silently failed on the client:

  * `database_setup.sql` uses `CREATE TABLE IF NOT EXISTS`. On a database that
    already has the table, EVERY later column addition is a no-op. MySQL/MariaDB
    do not add columns to an existing table, they skip the statement entirely.
  * `ensure_required_columns()` compensated with a hand-written
    `_REQUIRED_COLUMNS` map covering only 5 of the 8 schema tables, plus ~23
    literal ALTER statements sprinkled through main.py. Anything not listed
    there -- `rewards`, `redemptions`, `surveys`, `survey_responses`,
    `barangays` entirely -- was never created on an existing install.

Keeping a second hand-maintained list in sync with the SQL is what failed. So
this module treats `database_setup.sql` as the single source of truth: it parses
the CREATE TABLE definitions and reconciles the live database against them.

GUARANTEES (deliberately conservative)
--------------------------------------
  * ADDITIVE ONLY. It creates missing tables and adds missing columns. It never
    drops a table, never drops a column, never renames, never truncates.
  * A column whose type differs is REPORTED, not altered. Silently rewriting a
    column type on someone's production data is not worth the risk; the report
    entry is the signal that a human should decide.
  * Every statement is recorded so the result can be shown in the installer and
    in the server console.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("etrak.schema")

# Statement keywords that appear inside a CREATE TABLE body but are not columns.
_NON_COLUMN = re.compile(
    r"^(PRIMARY|UNIQUE|KEY|INDEX|CONSTRAINT|FOREIGN|FULLTEXT|SPATIAL|CHECK)\b",
    re.I,
)

# Backtick-quoted or bare identifier followed by a type name.
_COLUMN = re.compile(r"^[`\"]?(\w+)[`\"]?\s+([A-Za-z][A-Za-z0-9_]*(?:\s*\([^)]*\))?)")


@dataclass
class Column:
    name: str
    type: str
    raw: str


@dataclass
class Table:
    name: str
    columns: dict[str, Column]
    body: str


@dataclass
class MigrationReport:
    created_tables: list[str] = field(default_factory=list)
    added_columns: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    already_current: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.created_tables or self.added_columns)

    def summary(self) -> str:
        if self.errors and not self.changed:
            return f"schema check failed: {len(self.errors)} error(s)"
        parts = []
        if self.created_tables:
            parts.append(f"{len(self.created_tables)} table(s) created")
        if self.added_columns:
            parts.append(f"{len(self.added_columns)} column(s) added")
        if self.type_mismatches:
            parts.append(f"{len(self.type_mismatches)} type mismatch(es)")
        if self.errors:
            parts.append(f"{len(self.errors)} error(s)")
        return ", ".join(parts) if parts else "schema already up to date"

    def lines(self) -> list[str]:
        out = []
        for t in self.created_tables:
            out.append(f"  + table  {t}")
        for c in self.added_columns:
            out.append(f"  + column {c}")
        for t in self.type_mismatches:
            out.append(f"  ! type   {t}")
        for e in self.errors:
            out.append(f"  x error  {e}")
        return out


def parse_schema(sql_path: Path) -> dict[str, Table]:
    """Parse CREATE TABLE blocks into Table objects.

    The CREATE TABLE body is located by brace-matching from the opening
    parenthesis so that nested parentheses inside column types -- e.g.
    DECIMAL(10,2) -- do not truncate the statement.
    """
    src = Path(sql_path).read_text(encoding="utf-8")
    tables: dict[str, Table] = {}

    for match in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?(\w+)[`\"]?\s*\(",
        src,
        re.I,
    ):
        name = match.group(1)
        start = match.end() - 1
        depth = 0
        end = -1
        for i in range(start, len(src)):
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end == -1:
            log.warning("unterminated CREATE TABLE for %s", name)
            continue

        body = src[start + 1 : end]
        columns: dict[str, Column] = {}
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            if _NON_COLUMN.match(line):
                continue
            cm = _COLUMN.match(line)
            if cm:
                columns[cm.group(1)] = Column(cm.group(1), cm.group(2).upper(), line)
        if columns:
            tables[name] = Table(name, columns, body)

    return tables


def _normalise_type(sql_type: str) -> str:
    """Compare on the base type only: INT vs INT(11), VARCHAR(36) vs varchar."""
    t = re.sub(r"\s+", " ", sql_type.strip().upper())
    t = re.sub(r"\([^)]*\)", "", t)
    t = t.replace("UNSIGNED", "").strip()
    # MySQL aliases.
    aliases = {"INTEGER": "INT", "BOOL": "TINYINT", "BOOLEAN": "TINYINT"}
    return aliases.get(t, t)


def _is_sqlite(session) -> bool:
    try:
        return session.bind.dialect.name == "sqlite"
    except Exception:  # noqa: BLE001
        return False


def _sqlite_tables(session) -> set[str]:
    rows = session.execute(
        _text("SELECT name FROM sqlite_master WHERE type='table'")
    ).fetchall()
    return {r[0] for r in rows}


def _sqlite_columns(session, table: str) -> dict[str, str]:
    rows = session.execute(_text(f"PRAGMA table_info({table})")).fetchall()
    # PRAGMA table_info -> (cid, name, type, notnull, dflt_value, pk)
    return {r[1]: (r[2] or "").upper() for r in rows}


def _text(sql: str):
    from sqlalchemy import text

    return text(sql)


def create_index_if_missing(session, table: str, index: str, columns: str, *, unique=False) -> bool:
    """Create an index unless it already exists, on any supported dialect.

    SQLite understands `CREATE INDEX IF NOT EXISTS`; MySQL, MariaDB and TiDB
    reject it with a syntax error (1064). A raw error there used to abort the
    whole caller, so one index silently prevented every later table from being
    created on a fresh database. Ask the catalogue instead.
    """
    if _is_sqlite(session):
        kind = "UNIQUE INDEX" if unique else "INDEX"
        session.execute(
            _text(f"CREATE {kind} IF NOT EXISTS {index} ON {table}({columns})")
        )
        return True
    exists = session.execute(
        _text(
            "SELECT 1 FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND INDEX_NAME = :i"
        ),
        {"t": table, "i": index},
    ).first()
    if exists:
        return False
    kind = "UNIQUE INDEX" if unique else "INDEX"
    session.execute(_text(f"CREATE {kind} {index} ON {table}({columns})"))
    return True


def _column_ddl(column: Column) -> str:
    """Rebuild the DDL for a missing column.

    Uses the raw line from the schema and strips trailing table constraints so
    `ALTER TABLE ... ADD COLUMN` gets a valid fragment. Foreign keys and
    UNIQUE table constraints are dropped: adding them opportunistically to a
    live table can fail on existing data, and a plain column is enough for the
    application to work.
    """
    ddl = column.raw.strip().rstrip(",")
    ddl = re.sub(r"\s+(REFERENCES|CONSTRAINT|UNIQUE|FOREIGN|CHECK)\b.*$", "", ddl, flags=re.I)
    return ddl.strip()


def reconcile_mysql(session, schema_path: Path) -> MigrationReport:
    """Bring a live MySQL/MariaDB schema up to database_setup.sql. Additive only."""
    report = MigrationReport()
    tables = parse_schema(schema_path)
    if not tables:
        report.errors.append(f"no CREATE TABLE statements found in {schema_path}")
        return report

    live_rows = session.execute(
        _text(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()"
        )
    ).fetchall()
    live: dict[str, dict[str, str]] = {}
    for table_name, column_name, data_type in live_rows:
        live.setdefault(table_name, {})[column_name] = (data_type or "").upper()

    for name, table in tables.items():
        try:
            if name not in live:
                session.execute(_text(table_full_ddl(table, schema_path)))
                session.commit()
                report.created_tables.append(name)
                log.info("created table %s", name)
                continue

            live_cols = live[name]
            missing = [c for c in table.columns if c not in live_cols]
            for col_name in missing:
                column = table.columns[col_name]
                ddl = _column_ddl(column)
                try:
                    session.execute(
                        _text(f"ALTER TABLE `{name}` ADD COLUMN {ddl}")
                    )
                    session.commit()
                    report.added_columns.append(f"{name}.{col_name}")
                    log.info("added column %s.%s", name, col_name)
                except Exception as exc:  # noqa: BLE001
                    session.rollback()
                    msg = str(exc).splitlines()[0][:160]
                    report.errors.append(f"{name}.{col_name}: {msg}")

            if not missing:
                # Report (never fix) type drift.
                for col_name, column in table.columns.items():
                    want = _normalise_type(column.type)
                    got = (live_cols.get(col_name) or "").upper()
                    if got and _normalise_type(got) != want:
                        report.type_mismatches.append(
                            f"{name}.{col_name} (db={got}, schema={column.type})"
                        )
                report.already_current.append(name)
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            report.errors.append(f"{name}: {str(exc).splitlines()[0][:160]}")

    return report


def table_full_ddl(table: Table, schema_path: Path) -> str:
    """Reconstruct the original CREATE TABLE statement for a missing table."""
    src = Path(schema_path).read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?{re.escape(table.name)}[`\"]?\s*\(",
        src,
        re.I,
    )
    if not match:
        raise ValueError(f"cannot locate CREATE TABLE for {table.name}")
    start = match.end() - 1
    depth = 0
    for i in range(start, len(src)):
        if src[i] == "(":
            depth += 1
        elif src[i] == ")":
            depth -= 1
            if depth == 0:
                return src[match.start() : i + 1]
    raise ValueError(f"unterminated CREATE TABLE for {table.name}")


def reconcile_sqlite(session, schema_path: Path) -> MigrationReport:
    """SQLite equivalent. SQLite cannot add untyped constraints, so be careful."""
    report = MigrationReport()
    tables = parse_schema(schema_path)
    existing = _sqlite_tables(session)

    for name, table in tables.items():
        if name not in existing:
            ddl = table_full_ddl(table, schema_path)
            # Strip MySQL-only decorations SQLite rejects.
            ddl = re.sub(r"\bUNSIGNED\b", "", ddl, flags=re.I)
            ddl = re.sub(r"\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b", "", ddl, flags=re.I)
            ddl = re.sub(r"\bAUTO_INCREMENT\b", "", ddl, flags=re.I)
            ddl = re.sub(r",\s*(INDEX|KEY|CONSTRAINT|FOREIGN)\b[^,)]*", "", ddl, flags=re.I)
            try:
                session.execute(_text(ddl))
                session.commit()
                report.created_tables.append(name)
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                report.errors.append(f"{name}: {str(exc).splitlines()[0][:160]}")
            continue

        live_cols = _sqlite_columns(session, name)
        missing = [c for c in table.columns if c not in live_cols]
        for col_name in missing:
            column = table.columns[col_name]
            ddl = _column_ddl(column)
            ddl = re.sub(r"\bUNSIGNED\b", "", ddl, flags=re.I)
            ddl = re.sub(r"\bON\s+UPDATE\s+CURRENT_TIMESTAMP\b", "", ddl, flags=re.I)
            ddl = re.sub(r"\bAUTO_INCREMENT\b", "", ddl, flags=re.I)
            try:
                session.execute(_text(f'ALTER TABLE "{name}" ADD COLUMN {ddl}'))
                session.commit()
                report.added_columns.append(f"{name}.{col_name}")
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                report.errors.append(f"{name}.{col_name}: {str(exc).splitlines()[0][:160]}")
        if not missing:
            report.already_current.append(name)

    return report


def reconcile(session, schema_path: Path) -> MigrationReport:
    """Dispatch on dialect. Safe to call on every startup."""
    if _is_sqlite(session):
        return reconcile_sqlite(session, schema_path)
    return reconcile_mysql(session, schema_path)
