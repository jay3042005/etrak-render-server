from __future__ import annotations

try:
    import asyncio
    import glob
    import os

    # Load .env file for local development
    try:
        from dotenv import load_dotenv

        load_dotenv(os.path.join(os.getenv("ETRAK_DATA_DIR", os.path.dirname(__file__)), ".env"))
    except ImportError:
        pass
    import uuid
    import json
    import logging
    import mimetypes
    from datetime import datetime, date, timedelta, timezone
    from decimal import Decimal
    from timestamp_utils import (
        format_datetime_utc,
        report_created_at_column,
        truck_last_updated_column,
    )
    from db_config import database_url
    from lan_discovery import advertise_server
    from schedule_routes import collection_points as validate_points, route_name as validate_route_name, route_payload, schedule_scope, validate_driver, migrate_schedule_routes
    from schedule_routes import validate_schedule
    import pytz
    from typing import List, Optional

    from fastapi import (
        FastAPI, HTTPException, Depends, UploadFile, File, Response, Request,
    )
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
    from contextlib import asynccontextmanager
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from pydantic import BaseModel, validator
    import pymysql
    from zeroconf import ServiceInfo, ServiceBrowser, ServiceStateChange, Zeroconf
    import socket

    # Import security modules
    from jose import JWTError, jwt
    import bcrypt

    # Optional dependencies
    try:
        from zeroconf.asyncio import AsyncZeroconf
    except ImportError:
        pass
except Exception as e:
    print(f"DEBUG: Import error: {e}")
    raise

# Patch pydantic for compatibility
import sys

if "pydantic" in sys.modules:
    pass

# Imported at module level because the waste-classifier and analytics caches use
# them long before the later `import threading` further down the file executes.
import threading
from pathlib import Path

import pymysql
pymysql.install_as_MySQLdb()  # harmless for SQLite; required for mysql+pymysql:// URLs

# Additive schema reconciliation and the portable index helper both live here.
import schema_sync


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== STARTUP ==========
    global _truck_status_task, _schedule_notify_task, _mdns_task

    # Start mDNS registration first to avoid late-discovery race on clients
    if _mdns_task is None or _mdns_task.done():
        _mdns_task = asyncio.create_task(register_mdns())

    # Start truck status sweeper
    if _truck_status_task is None or _truck_status_task.done():
        _truck_status_task = asyncio.create_task(_truck_status_sweeper())
        logging.info(
            "Started truck status sweeper: timeout=%ss interval=%ss",
            TRUCK_ACTIVE_TIMEOUT_SECONDS,
            TRUCK_STATUS_SWEEP_INTERVAL_SECONDS,
        )

    # Start schedule notification sender
    if SCHEDULE_NOTIFY_ENABLED:
        if _schedule_notify_task is None or _schedule_notify_task.done():
            _schedule_notify_task = asyncio.create_task(_schedule_notification_sender())
            logging.info(
                "Started schedule notification sender: hours_before=%s interval=%smin",
                SCHEDULE_NOTIFY_HOURS_BEFORE,
                SCHEDULE_NOTIFY_INTERVAL_MINUTES,
            )

    # Schema reconciliation, ALWAYS on.
    #
    # This replaces the old database_setup.sql-only path, which could not update
    # an existing database: CREATE TABLE IF NOT EXISTS is a no-op once the table
    # exists, so every later column was silently ignored. schema_sync derives the
    # truth from database_setup.sql and is strictly additive (create missing
    # tables, add missing columns) -- it never drops or rewrites anything.
    await asyncio.to_thread(run_schema_sync)
    await asyncio.to_thread(migrate_schedule_routes, engine)

    # Required-column repair, ALWAYS on, and deliberately AFTER schema_sync.
    #
    # schema_sync can only add what database_setup.sql declares. Several columns
    # the request handlers write were never added to that file -- truck_locations
    # .is_on_duty and users.phone among them -- so schema_sync alone left them
    # missing on every existing install. The truck status sweeper then crashed
    # every few seconds with:
    #     (1054, "Unknown column 'is_on_duty' in 'WHERE'")
    # and the driver duty toggle silently never persisted.
    #
    # This function was written for exactly this, but lost its only call site
    # when schema_sync replaced the old startup path, so the safety net was
    # defined and never run. It is additive-only as well.
    try:
        await asyncio.to_thread(ensure_required_columns)
    except Exception as exc:  # noqa: BLE001
        logging.error("Required-column repair failed: %s", str(exc).splitlines()[0][:200])

    # Database setup on startup (disabled by default in production). Kept only
    # for backwards compatibility; schema_sync above is now authoritative.
    if DB_SETUP_ON_STARTUP:
        if not DATABASE_URL or not engine:
            print("Warning: No database connection found on startup.")

        try:
            setup_database()
        except Exception as e:
            print(f"[!] Database setup error: {e}")

        try:
            run_migrations()
        except Exception as e:
            print(f"[!] Migration error: {e}")
    else:
        logging.info("Skipping startup DB setup (DB_SETUP_ON_STARTUP=false)")

    yield  # App is running

    # ========== SHUTDOWN ==========
    if _truck_status_task is not None:
        _truck_status_task.cancel()
        try:
            await _truck_status_task
        except asyncio.CancelledError:
            pass
        _truck_status_task = None

    if _schedule_notify_task is not None:
        _schedule_notify_task.cancel()
        try:
            await _schedule_notify_task
        except asyncio.CancelledError:
            pass
        _schedule_notify_task = None

    if _mdns_task is not None:
        _mdns_task.cancel()
        try:
            await _mdns_task
        except asyncio.CancelledError:
            pass
        _mdns_task = None


DATA_DIR = os.getenv("ETRAK_DATA_DIR", os.path.dirname(__file__))
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# API docs expose every route (incl. admin DB tools); off unless ENABLE_API_DOCS=true.
_DOCS_ENABLED = os.getenv("ENABLE_API_DOCS", "false").lower() == "true"
app = FastAPI(
    title="E-Trak Yan API",
    lifespan=lifespan,
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
)

# The driver app pings GPS every 30s. Allow several consecutive misses before
# declaring a truck stale: on mobile data a single dropped ping, a slow GPS fix
# or a brief tunnel blip is normal, and a too-tight window makes the truck
# flap offline mid-shift.
TRUCK_ACTIVE_TIMEOUT_SECONDS = int(os.getenv("TRUCK_ACTIVE_TIMEOUT_SECONDS", "300"))
TRUCK_STATUS_SWEEP_INTERVAL_SECONDS = int(
    os.getenv("TRUCK_STATUS_SWEEP_INTERVAL_SECONDS", "5")
)
FLEET_LIVE_CACHE_SECONDS = int(os.getenv("FLEET_LIVE_CACHE_SECONDS", "15"))
# A device is "online" if it has pinged within this window. The app heartbeats
# every ~60s, so a couple of missed beats is already suspicious.
DEVICE_ONLINE_TIMEOUT_SECONDS = int(os.getenv("DEVICE_ONLINE_TIMEOUT_SECONDS", "180"))
TRUCK_LOCATIONS_CACHE_SECONDS = int(os.getenv("TRUCK_LOCATIONS_CACHE_SECONDS", "10"))
REPORTS_CACHE_SECONDS = int(os.getenv("REPORTS_CACHE_SECONDS", "10"))
BARANGAY_RANK_CACHE_SECONDS = int(os.getenv("BARANGAY_RANK_CACHE_SECONDS", "60"))
_truck_status_task = None
_mdns_task = None
_fleet_live_cache = {"data": None, "timestamp": None}
_truck_locations_cache = {}
_reports_cache = {}
_barangay_rank_cache = {"data": None, "timestamp": None}

PH_TZ = pytz.timezone("Asia/Manila")


def get_ph_time():
    return datetime.now(PH_TZ)


def get_ph_weekday():
    return get_ph_time().weekday()


def _parse_schedule_time(value):
    """Accept 'HH:MM', 'HH:MM:SS' and datetime.time values from the DB."""
    if value is None:
        return None
    text = str(value).strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def _same_schedule_time(left, right):
    """Compare times without treating 06:00 and 06:00:00 as an edit."""
    a = _parse_schedule_time(left)
    b = _parse_schedule_time(right)
    if a is not None and b is not None:
        return a.replace(second=0, microsecond=0) == b.replace(
            second=0, microsecond=0
        )
    return str(left or "")[:5] == str(right or "")[:5]


# JWT Secret - should be set via environment variable in production
JWT_SECRET = os.getenv("JWT_SECRET", "etrak-yan-secret-key-change-in-production")
# Default to True for embedded mobile environment; set to false to skip DB setup on startup
DB_SETUP_ON_STARTUP = os.getenv("DB_SETUP_ON_STARTUP", "true").lower() == "true"


def _normalize_pagination(limit: int, offset: int, *, default_limit: int = 100, max_limit: int = 200):
    safe_limit = limit if isinstance(limit, int) else default_limit
    safe_offset = offset if isinstance(offset, int) else 0

    if safe_limit <= 0:
        safe_limit = default_limit
    if safe_limit > max_limit:
        safe_limit = max_limit
    if safe_offset < 0:
        safe_offset = 0

    return safe_limit, safe_offset


def _is_sqlite(db) -> bool:
    return db.get_bind().dialect.name == "sqlite"


# Manual admin entries use sentinels instead of NULL because some deployed
# databases declare collection_records.driver_id and schedule_id NOT NULL.
MANUAL_ENTRY_DRIVER_ID = "MANUAL_ADMIN"
MANUAL_ENTRY_SCHEDULE_ID = "MANUAL_ADMIN"


def _driver_truck_match_sql(db) -> str:
    """SQL condition matching a truck_locations row to a driver user row.

    Older app builds sent DRIVER_<first 8 chars of user id>; newer builds let
    the server store the full user id. Accept both without duplicating trucks.
    """
    if _is_sqlite(db):
        return (
            "(u.id = tl.truck_id OR "
            "substr(u.id, 1, 8) = replace(tl.truck_id, 'DRIVER_', ''))"
        )
    return (
        "(u.id = tl.truck_id OR "
        "LEFT(u.id, 8) = REPLACE(tl.truck_id, 'DRIVER_', ''))"
    )


def _store_app_update(db, version: str, url: str, force: bool) -> None:
    """Insert or update the single app_update row without dialect-specific upserts."""
    params = {"version": version, "url": url, "force": force}
    existing = db.execute(
        text("SELECT id FROM app_update WHERE id = 'app'")
    ).fetchone()
    if existing:
        db.execute(
            text("""
                UPDATE app_update
                SET latest_version = :version,
                    update_url = :url,
                    force_update = :force,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 'app'
            """),
            params,
        )
    else:
        db.execute(
            text("""
                INSERT INTO app_update
                    (id, latest_version, update_url, force_update, updated_at)
                VALUES ('app', :version, :url, :force, CURRENT_TIMESTAMP)
            """),
            params,
        )


def _cache_get(cache_store: dict, cache_key: str, ttl_seconds: int):
    if ttl_seconds <= 0:
        return None
    entry = cache_store.get(cache_key)
    if not entry:
        return None
    ts = entry.get("timestamp")
    data = entry.get("data")
    if ts is None or data is None:
        return None
    if (datetime.now() - ts).total_seconds() > ttl_seconds:
        return None
    return data


def _cache_set(cache_store: dict, cache_key: str, data):
    cache_store[cache_key] = {"timestamp": datetime.now(), "data": data}


async def get_current_user_record(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False)),
) -> dict:
    """Extract and validate user from JWT token."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        # Get user from database
        db = SessionLocal()
        try:
            result = db.execute(
                text(
                    "SELECT id, email, full_name, role, barangay, is_active, assigned_truck_plate FROM users WHERE id = :id"
                ),
                {"id": user_id},
            ).fetchone()
            if result is None:
                raise HTTPException(status_code=401, detail="User not found")
            # Admin "toggle-status" sets is_active = false; honour it here so a
            # disabled account's existing token stops working. NULL = active.
            if result[5] is not None and not result[5]:
                raise HTTPException(status_code=403, detail="Account disabled")

            return {
                "id": result[0],
                "email": result[1],
                "full_name": result[2],
                "role": result[3],
                "barangay": result[4],
                "is_active": result[5],
                "assigned_truck_plate": result[6],
            }
        finally:
            db.close()
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def _mark_stale_trucks_offline(db):
    result = db.execute(
        text(
            """
            UPDATE truck_locations
            SET status = 'offline'
            WHERE status <> 'offline'
              AND (is_on_duty IS NULL OR is_on_duty = FALSE)
              AND last_updated IS NOT NULL
              AND last_updated < :cutoff
            """
        ),
        {"cutoff": datetime.now() - timedelta(seconds=TRUCK_ACTIVE_TIMEOUT_SECONDS)},
    )
    if result.rowcount:
        logging.info(
            "Marked %s stale truck(s) offline using %ss timeout",
            result.rowcount,
            TRUCK_ACTIVE_TIMEOUT_SECONDS,
        )
    db.commit()


async def _truck_status_sweeper():
    while True:
        db = SessionLocal()
        try:
            _mark_stale_trucks_offline(db)
        except Exception as e:
            logging.exception("Truck status sweeper failed: %s", e)
            db.rollback()
        finally:
            db.close()

        await asyncio.sleep(TRUCK_STATUS_SWEEP_INTERVAL_SECONDS)


def _effective_truck_status(status, last_updated, is_on_duty=None):
    normalized_status = (status or "offline").lower()
    if normalized_status == "offline":
        return "offline"

    if last_updated is None:
        return "offline"

    if isinstance(last_updated, str):
        try:
            last_updated = datetime.fromisoformat(
                last_updated.strip().replace("Z", "+00:00")
            )
        except ValueError:
            return "offline"

    if isinstance(last_updated, (int, float, Decimal)):
        last_updated = datetime.fromtimestamp(float(last_updated), tz=timezone.utc)

    if not isinstance(last_updated, datetime):
        return "offline"

    # Normalise to UTC so the comparison below is between two aware instants.
    # A naive value from MySQL is a Manila wall clock, not UTC.
    if last_updated.tzinfo is None:
        last_updated = PH_TZ.localize(last_updated).astimezone(timezone.utc)
    else:
        last_updated = last_updated.astimezone(timezone.utc)

    # An explicit duty toggle outranks staleness. A driver whose phone briefly
    # lost signal mid-shift is still on duty; only go-offline ends it.
    if is_on_duty:
        return normalized_status

    if datetime.now(timezone.utc) - last_updated > timedelta(
        seconds=TRUCK_ACTIVE_TIMEOUT_SECONDS
    ):
        return "offline"

    return normalized_status


def _serialize_truck_location(row):
    mapping = row._mapping
    last_updated = mapping.get("last_updated")
    # Staleness must be judged on the real instant. On MySQL the column arrives
    # as a UTC epoch (see truck_last_updated_column); comparing a naive local
    # datetime against datetime.now() is what made the 300s lease unreachable.
    last_updated_aware = None
    if last_updated is not None:
        if isinstance(last_updated, datetime):
            last_updated_aware = (
                last_updated
                if last_updated.tzinfo is not None
                else PH_TZ.localize(last_updated).astimezone(timezone.utc)
            )
        elif isinstance(last_updated, (int, float, Decimal)):
            last_updated_aware = datetime.fromtimestamp(
                float(last_updated), tz=timezone.utc
            )
        elif isinstance(last_updated, str):
            try:
                parsed = datetime.fromisoformat(
                    last_updated.strip().replace("Z", "+00:00")
                )
                last_updated_aware = (
                    parsed
                    if parsed.tzinfo is not None
                    else PH_TZ.localize(parsed).astimezone(timezone.utc)
                )
            except ValueError:
                last_updated_aware = None

    return {
        "id": mapping.get("id"),
        "truck_id": mapping.get("truck_id"),
        "driver_name": mapping.get("driver_name"),
        "plate_number": mapping.get("plate_number"),
        "latitude": mapping.get("latitude"),
        "longitude": mapping.get("longitude"),
        "current_barangay": mapping.get("current_barangay"),
        "status": _effective_truck_status(
            mapping.get("status"), last_updated_aware, mapping.get("is_on_duty")
        ),
        "speed": mapping.get("speed"),
        "last_updated": format_datetime_utc(last_updated),
    }


# ----------------------------------------------------------------------
# Simple health‑check / ping endpoint
# ----------------------------------------------------------------------
def _require_self_or_admin(current_user: dict, user_id: str):
    if current_user.get("role") != "admin" and str(current_user.get("id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Not allowed")


async def require_admin(current_user: dict = Depends(get_current_user_record)):
    """Dependency: the caller must be an authenticated admin."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return current_user


@app.get("/ping")
async def ping():
    """A tiny endpoint used to verify the service is up."""
    return {"message": "pong"}


# ----------------------------------------------------------------------
# Authentication endpoints
# ----------------------------------------------------------------------
@app.post("/api/auth/login")
async def login(payload: dict, request: Request):
    """Authenticate user and return JWT token."""
    db = SessionLocal()
    try:
        email = payload.get("email", "").strip().lower()
        password = payload.get("password", "")

        if not email or not password:
            raise HTTPException(
                status_code=400, detail="Email and password are required"
            )

        result = db.execute(
            text("""
                SELECT id, email, full_name, password, role, barangay, is_active, assigned_truck_plate
                FROM users WHERE email = :email
            """),
            {"email": email},
        ).fetchone()

        if not result:
            raise HTTPException(status_code=401, detail="Invalid email or password")

        (
            user_id,
            user_email,
            full_name,
            hashed_password,
            role,
            barangay,
            is_active,
            assigned_truck,
        ) = result

        if is_active is not None and not is_active:  # MySQL returns 0, not False
            raise HTTPException(status_code=403, detail="Account is disabled")

        if not bcrypt.checkpw(password.encode(), hashed_password.encode()):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token_data = {"sub": user_id, "email": user_email, "role": role}
        token = jwt.encode(token_data, JWT_SECRET, algorithm="HS256")

        # Remember this device so admins can see who was last active where.
        # A failure here must never block a login.
        try:
            _record_device(
                db,
                user_id=user_id,
                email=user_email,
                full_name=full_name,
                role=role,
                payload=payload,
                headers=request.headers,
                ip=_client_ip(request),
            )
        except Exception as exc:
            logging.getLogger("etrak_server").warning(
                "device registration failed for %s: %s", user_email, exc
            )

        return {
            "success": True,
            "data": {
                "token": token,
                "user": {
                    "id": user_id,
                    "email": user_email or "",
                    "full_name": full_name or "",
                    "role": role or "resident",
                    "barangay": barangay or "Unknown",
                    "is_active": is_active if is_active is not None else True,
                    "assigned_truck_plate": assigned_truck,
                },
            },
        }
    finally:
        db.close()


# ============= DEVICE PRESENCE =============

DEVICE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS devices (
        device_id VARCHAR(128) PRIMARY KEY,
        user_id VARCHAR(36),
        email VARCHAR(255),
        full_name VARCHAR(100),
        role VARCHAR(20),
        platform VARCHAR(40),
        app_version VARCHAR(40),
        device_label VARCHAR(120),
        last_seen TIMESTAMP NULL,
        first_seen TIMESTAMP NULL,
        last_ip VARCHAR(64)
    )
"""

_devices_table_ready = False


def _ensure_devices_table(db):
    """Create the devices table once per process."""
    global _devices_table_ready
    if _devices_table_ready:
        return
    db.execute(text(DEVICE_TABLE_SQL))
    db.commit()
    _devices_table_ready = True


def _client_ip(request: Request) -> str:
    """Real client IP, honouring the Cloudflare/proxy header."""
    forwarded = request.headers.get("cf-connecting-ip") or request.headers.get(
        "x-forwarded-for"
    )
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    return (request.client.host if request.client else "")[:64]


def _record_device(
    db,
    *,
    user_id,
    email=None,
    full_name=None,
    role=None,
    payload=None,
    headers=None,
    ip="",
):
    """Upsert the calling device. Called on login and on heartbeat."""
    payload = payload or {}
    headers = headers or {}

    device_id = (
        payload.get("device_id")
        or headers.get("x-device-id")
        or f"web-{ip or 'unknown'}"
    )[:128]
    platform = (payload.get("platform") or headers.get("x-platform") or "")[:40]
    app_version = (payload.get("app_version") or headers.get("x-app-version") or "")[:40]
    label = (
        payload.get("device_label")
        or payload.get("device_name")
        or headers.get("x-device-label")
        or ""
    )[:120]

    _ensure_devices_table(db)
    now = datetime.now()
    existing = db.execute(
        text("SELECT first_seen FROM devices WHERE device_id = :d"), {"d": device_id}
    ).fetchone()

    if existing is None:
        db.execute(
            text(
                """
                INSERT INTO devices (device_id, user_id, email, full_name, role,
                                     platform, app_version, device_label,
                                     last_seen, first_seen, last_ip)
                VALUES (:d, :u, :e, :n, :r, :p, :v, :l, :ls, :fs, :ip)
                """
            ),
            {
                "d": device_id, "u": user_id, "e": email, "n": full_name,
                "r": role, "p": platform, "v": app_version, "l": label,
                "ls": now, "fs": now, "ip": ip or None,
            },
        )
    else:
        # COALESCE keeps previously-known identity if this call only has a token.
        db.execute(
            text(
                """
                UPDATE devices SET
                    user_id = COALESCE(:u, user_id),
                    email = COALESCE(:e, email),
                    full_name = COALESCE(:n, full_name),
                    role = COALESCE(:r, role),
                    platform = CASE WHEN :p <> '' THEN :p ELSE platform END,
                    app_version = CASE WHEN :v <> '' THEN :v ELSE app_version END,
                    device_label = CASE WHEN :l <> '' THEN :l ELSE device_label END,
                    last_seen = :ls,
                    last_ip = COALESCE(:ip, last_ip)
                WHERE device_id = :d
                """
            ),
            {
                "d": device_id, "u": user_id, "e": email, "n": full_name,
                "r": role, "p": platform, "v": app_version, "l": label,
                "ls": now, "ip": ip or None,
            },
        )
    db.commit()
    return device_id


def _parse_device_datetime(value):
    """Normalize DB timestamp values; SQLite text queries return strings."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _device_age_seconds(last_seen):
    last_seen = _parse_device_datetime(last_seen)
    if last_seen is None:
        return None
    now = datetime.now(last_seen.tzinfo) if last_seen.tzinfo else datetime.now()
    return (now - last_seen).total_seconds()


def _device_status(last_seen):
    """online / idle / offline, from how long ago the device last phoned home."""
    age = _device_age_seconds(last_seen)
    if age is None:
        return "offline"
    if age <= DEVICE_ONLINE_TIMEOUT_SECONDS:
        return "online"
    if age <= DEVICE_ONLINE_TIMEOUT_SECONDS * 10:
        return "idle"
    return "offline"


@app.post("/api/devices/heartbeat")
async def device_heartbeat(
    request: Request,
    current_user: dict = Depends(get_current_user_record),
):
    """Ping from a signed-in app so admins can see it as active."""
    db = SessionLocal()
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        _record_device(
            db,
            user_id=current_user.get("id"),
            email=current_user.get("email"),
            full_name=current_user.get("full_name"),
            role=current_user.get("role"),
            payload=payload if isinstance(payload, dict) else {},
            headers=request.headers,
            ip=_client_ip(request),
        )
        return {"success": True, "online_timeout": DEVICE_ONLINE_TIMEOUT_SECONDS}
    finally:
        db.close()


@app.get("/api/admin/devices")
async def list_devices(current_user: dict = Depends(get_current_user_record)):
    """Every device that has ever signed in, newest activity first."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    db = SessionLocal()
    try:
        _ensure_devices_table(db)
        rows = db.execute(
            text(
                """
                SELECT device_id, user_id, email, full_name, role, platform,
                       app_version, device_label, last_seen, first_seen, last_ip
                FROM devices
                ORDER BY last_seen DESC
                """
            )
        ).fetchall()
        data = []
        for r in rows:
            last_seen = _parse_device_datetime(r[8])
            first_seen = _parse_device_datetime(r[9])
            age = _device_age_seconds(last_seen)
            data.append({
                "device_id": r[0],
                "user_id": r[1],
                "email": r[2] or "",
                "full_name": r[3] or "",
                "role": r[4] or "",
                "platform": r[5] or "",
                "app_version": r[6] or "",
                "device_label": r[7] or "",
                "last_seen": last_seen.isoformat() if last_seen else None,
                "first_seen": first_seen.isoformat() if first_seen else None,
                "last_ip": r[10] or "",
                "status": _device_status(last_seen),
                "seconds_since_seen": int(age) if age is not None else None,
            })
        summary = {
            "total": len(data),
            "online": sum(1 for d in data if d["status"] == "online"),
            "idle": sum(1 for d in data if d["status"] == "idle"),
            "offline": sum(1 for d in data if d["status"] == "offline"),
        }
        return {"success": True, "data": data, "summary": summary}
    finally:
        db.close()


@app.post("/api/admin/devices/{device_id}/logout")
async def device_force_logout(
    device_id: str, current_user: dict = Depends(get_current_user_record)
):
    """Mark a device as signed out (it stops counting as active)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    db = SessionLocal()
    try:
        _ensure_devices_table(db)
        db.execute(
            text("UPDATE devices SET last_seen = :old WHERE device_id = :d"),
            {
                "d": device_id[:128],
                "old": datetime.now()
                - timedelta(seconds=DEVICE_ONLINE_TIMEOUT_SECONDS * 20),
            },
        )
        db.commit()
        return {"success": True}
    finally:
        db.close()


@app.post("/api/auth/register")
async def register(request: dict):
    """Register a new user."""
    db = SessionLocal()
    try:
        email = request.get("email", "").strip().lower()
        password = request.get("password", "")
        full_name = request.get("full_name", "")
        barangay = request.get("barangay", None)

        if not email or not password or not full_name:
            raise HTTPException(
                status_code=400, detail="Email, password, and full name are required"
            )

        existing = db.execute(
            text("SELECT id FROM users WHERE email = :email"),
            {"email": email},
        ).fetchone()

        if existing:
            raise HTTPException(status_code=400, detail="Email already registered")

        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = str(uuid.uuid4())

        db.execute(
            text("""
                INSERT INTO users (id, email, full_name, password, role, barangay, is_active, created_at)
                VALUES (:id, :email, :full_name, :password, 'resident', :barangay, TRUE, CURRENT_TIMESTAMP)
            """),
            {
                "id": user_id,
                "email": email,
                "full_name": full_name,
                "password": hashed,
                "barangay": barangay,
            },
        )
        db.commit()

        # Auto-login after registration
        token_data = {"sub": user_id, "email": email, "role": "resident"}
        token = jwt.encode(token_data, JWT_SECRET, algorithm="HS256")

        return {
            "success": True,
            "data": {
                "token": token,
                "user": {
                    "id": user_id,
                    "email": email,
                    "full_name": full_name,
                    "role": "resident",
                    "barangay": barangay,
                    "is_active": True,
                    "assigned_truck_plate": None,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/auth/profile")
async def get_profile(current_user: dict = Depends(get_current_user_record)):
    """Get current user profile."""
    return {
        "success": True,
        "data": {
            "user": {
                "id": current_user["id"],
                "email": current_user["email"] or "",
                "full_name": current_user["full_name"] or "",
                "role": current_user["role"] or "resident",
                "barangay": current_user["barangay"] or "Unknown",
                "is_active": current_user["is_active"]
                if current_user["is_active"] is not None
                else True,
                "assigned_truck_plate": current_user["assigned_truck_plate"],
            },
        },
    }


@app.get("/api/auth/profile/stats")
async def get_profile_stats(
    userId: str = None,
    barangay: str = None,
    current_user: dict = Depends(get_current_user_record),
):
    """Get user stats for profile page."""
    db = SessionLocal()
    try:
        target_user_id = userId or current_user.get("id")
        target_barangay = barangay or current_user.get("barangay")

        user_result = db.execute(
            text(
                "SELECT total_points, lifetime_points, reports_submitted FROM users WHERE id = :id"
            ),
            {"id": target_user_id},
        ).fetchone()

        if not user_result:
            raise HTTPException(status_code=404, detail="User not found")

        total_points = user_result[0] if user_result[0] is not None else 0
        lifetime_points = user_result[1] if user_result[1] is not None else 0
        reports_submitted = user_result[2] if user_result[2] is not None else 0

        completed_collections = (
            db.execute(
                text(
                    "SELECT COUNT(*) FROM collection_records WHERE driver_id = :id AND status IN ('completed', 'collected')"
                ),
                {"id": target_user_id},
            ).fetchone()[0]
            or 0
        )

        # Compute city rank by barangay standings (same logic as /api/barangay-scores)
        city_rank = None
        if target_barangay:
            rankings = _cache_get(_barangay_rank_cache, "rankings", BARANGAY_RANK_CACHE_SECONDS)
            if rankings is None:
                barangay_scores = db.execute(
                    text("""
                        SELECT
                            barangay as barangay_name,
                            SUM(total_points) as total_points,
                            COUNT(id) as resident_count
                        FROM users
                        WHERE role = 'resident' AND barangay IS NOT NULL
                        GROUP BY barangay
                    """)
                ).fetchall()

                rankings = []
                for row in barangay_scores:
                    m = row._mapping
                    bp = int(m.get("total_points", 0) or 0)
                    rc = int(m.get("resident_count", 0) or 0)
                    ppr = bp / max(1, rc)
                    rankings.append({
                        "barangay": m.get("barangay_name"),
                        "points_per_resident": ppr,
                        "total_points": bp,
                    })

                rankings.sort(key=lambda x: (x["points_per_resident"], x["total_points"]), reverse=True)
                _cache_set(_barangay_rank_cache, "rankings", rankings)

            for i, r in enumerate(rankings):
                if r["barangay"] == target_barangay:
                    city_rank = i + 1
                    break

        return {
            "success": True,
            "data": {
                "totalPoints": total_points,
                "lifetimePoints": lifetime_points,
                "reportsSubmitted": reports_submitted,
                "completedCollections": completed_collections,
                "rank": city_rank,
            },
        }
    finally:
        db.close()


@app.put("/api/auth/profile")
async def update_profile(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Update current user profile."""
    db = SessionLocal()
    try:
        barangay = request.get("barangay")
        phone = request.get("phoneNumber")

        update_fields = []
        params = {"id": current_user["id"]}

        if barangay is not None:
            update_fields.append("barangay = :barangay")
            params["barangay"] = barangay

        if phone is not None:
            update_fields.append("phone = :phone")
            params["phone"] = phone

        if update_fields:
            db.execute(
                text(f"UPDATE users SET {', '.join(update_fields)} WHERE id = :id"),
                params,
            )
            db.commit()

        # Return updated user
        result = db.execute(
            text(
                "SELECT id, email, full_name, role, barangay, is_active, assigned_truck_plate FROM users WHERE id = :id"
            ),
            {"id": current_user["id"]},
        ).fetchone()

        if result:
            return {
                "success": True,
                "data": {
                    "user": {
                        "id": result[0],
                        "email": result[1],
                        "full_name": result[2],
                        "role": result[3],
                        "barangay": result[4],
                        "is_active": bool(result[5]) if result[5] is not None else True,
                        "assigned_truck_plate": result[6],
                    },
                },
            }
        raise HTTPException(status_code=404, detail="User not found")
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/users")
async def get_users(
    role: str = None,
    limit: int = 100,
    offset: int = 0,
    current_user: dict = Depends(get_current_user_record),
):
    """Get all users (admin only), optionally filtered by role."""
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=200)
        if role:
            result = db.execute(
                text("""
                    SELECT id, email, full_name, role, barangay, is_active, assigned_truck_plate
                    FROM users WHERE role = :role ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """),
                {"role": role, "limit": limit, "offset": offset}
            ).fetchall()
        else:
            result = db.execute(
                text("""
                    SELECT id, email, full_name, role, barangay, is_active, assigned_truck_plate
                    FROM users ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """)
                ,
                {"limit": limit, "offset": offset},
            ).fetchall()

        users = []
        for row in result:
            users.append(
                {
                    "id": row[0],
                    "email": row[1],
                    "full_name": row[2],
                    "role": row[3],
                    "barangay": row[4],
                    "is_active": bool(row[5]) if row[5] is not None else True,
                    "assigned_truck_plate": row[6],
                }
            )
        return {"success": True, "data": users}
    finally:
        db.close()


# Schedule Notification Settings
SCHEDULE_NOTIFY_HOURS_BEFORE = int(os.getenv("SCHEDULE_NOTIFY_HOURS_BEFORE", "2"))
SCHEDULE_NOTIFY_INTERVAL_MINUTES = int(
    os.getenv("SCHEDULE_NOTIFY_INTERVAL_MINUTES", "60")
)
SCHEDULE_NOTIFY_ENABLED = os.getenv("SCHEDULE_NOTIFY_ENABLED", "true").lower() == "true"
_schedule_notify_task = None


def _get_upcoming_schedules_for_notification(db):
    current_weekday = get_ph_weekday()

    result = db.execute(
        text("""
            SELECT s.id, s.barangay, s.day_of_week, s.start_time, s.collection_type,
                   u.id as driver_id, u.full_name as driver_name
            FROM schedules s
            LEFT JOIN users u ON s.driver_id = u.id
            WHERE s.is_active = TRUE
        """)
    ).fetchall()

    upcoming = []
    now = get_ph_time()

    for row in result:
        schedule_id = row[0]
        barangay = row[1]
        day_of_week = row[2]
        start_time = row[3]
        collection_type = row[4]
        driver_id = row[5]
        driver_name = row[6]

        schedule_time = _parse_schedule_time(start_time)
        if schedule_time is None:
            continue

        days_until = (day_of_week - current_weekday) % 7
        if days_until == 0:
            schedule_datetime = now.replace(
                hour=schedule_time.hour,
                minute=schedule_time.minute,
                second=0,
                microsecond=0,
            )
            if schedule_datetime <= now:
                days_until = 7
                schedule_datetime = schedule_datetime + timedelta(days=7)
        else:
            schedule_datetime = now + timedelta(days=days_until)
            schedule_datetime = schedule_datetime.replace(
                hour=schedule_time.hour,
                minute=schedule_time.minute,
                second=0,
                microsecond=0,
            )

        hours_until = (schedule_datetime - now).total_seconds() / 3600

        if 0 < hours_until <= SCHEDULE_NOTIFY_HOURS_BEFORE:
            cutoff_24h = get_ph_time().replace(tzinfo=None) - timedelta(hours=24)
            # Check for an existing broadcast (or legacy per-user) notification
            # for this schedule within the last 24 hours.
            already_notified = db.execute(
                text("""
                    SELECT COUNT(*) FROM notifications
                    WHERE (schedule_id = :schedule_id
                           OR (schedule_id IS NULL AND title LIKE :title_pattern))
                    AND created_at > :cutoff
                """),
                {
                    "schedule_id": schedule_id,
                    "title_pattern": f"%{barangay}%",
                    "cutoff": cutoff_24h,
                },
            ).fetchone()[0]

            if already_notified == 0:
                upcoming.append(
                    {
                        "id": schedule_id,
                        "barangay": barangay,
                        "day_of_week": day_of_week,
                        "start_time": start_time,
                        "collection_type": collection_type or "All",
                        "driver_id": driver_id,
                        "driver_name": driver_name,
                        "hours_until": hours_until,
                    }
                )

    return upcoming


def _send_schedule_notification(db, schedule_info):
    """Insert a single broadcast notification for the barangay schedule.

    Instead of looping over every resident and inserting one row per user,
    we store one row with target_scope='barangay' and target_barangay set.
    The GET endpoint resolves matching users at read time, so new residents
    who register after this notification was created will still see it.
    """
    barangay = schedule_info["barangay"]
    start_time = schedule_info["start_time"]
    collection_type = schedule_info["collection_type"]
    hours_until = schedule_info["hours_until"]
    schedule_id = schedule_info["id"]

    if hours_until <= 1:
        title = "Garbage Collection Starting Soon!"
        body = f"Garbage collection for {barangay} will begin within the hour ({collection_type} waste)."
    else:
        title = "Upcoming Garbage Collection"
        body = f"Garbage collection for {barangay} is scheduled at {start_time} ({collection_type} waste)."

    notification_count = 0

    # One broadcast row for all residents in this barangay
    notif_id = str(uuid.uuid4())
    try:
        db.execute(
            text("""
                INSERT INTO notifications
                (id, user_id, title, body, type, is_read,
                 target_scope, target_barangay, schedule_id, created_at)
                VALUES (:id, NULL, :title, :body, :type, FALSE,
                        'barangay', :barangay, :schedule_id, CURRENT_TIMESTAMP)
            """),
            {
                "id": notif_id,
                "title": title,
                "body": body,
                "type": "collection",
                "barangay": barangay,
                "schedule_id": schedule_id,
            },
        )
        notification_count += 1
    except Exception as e:
        logging.warning(f"Failed to send broadcast notification for {barangay}: {e}")

    # Drivers get a direct per-user reminder so it appears in their own list
    # even if they live in a different barangay.
    driver_id = schedule_info.get("driver_id")
    if driver_id:
        try:
            db.execute(
                text("""
                    INSERT INTO notifications 
                    (id, user_id, title, body, type, is_read, schedule_id, created_at)
                    VALUES (:id, :user_id, :title, :body, :type, FALSE, :schedule_id, CURRENT_TIMESTAMP)
                """),
                {
                    "id": str(uuid.uuid4()),
                    "user_id": driver_id,
                    "title": f"Shift reminder: {barangay}",
                    "body": f"Your {collection_type} shift in {barangay} starts at {start_time}.",
                    "type": "shift",
                    "schedule_id": schedule_id,
                },
            )
            notification_count += 1
        except Exception as e:
            logging.warning(
                f"Failed to send shift reminder to driver {driver_id}: {e}"
            )

    if notification_count > 0:
        db.commit()
        logging.info(
            f"Sent broadcast schedule notification for {barangay} (1 row, visible to all residents)"
        )

    return notification_count


async def _schedule_notification_sender():
    while True:
        if not SCHEDULE_NOTIFY_ENABLED:
            await asyncio.sleep(SCHEDULE_NOTIFY_INTERVAL_MINUTES * 60)
            continue

        db = SessionLocal()
        try:
            upcoming_schedules = _get_upcoming_schedules_for_notification(db)
            for schedule in upcoming_schedules:
                try:
                    _send_schedule_notification(db, schedule)
                except Exception as e:
                    logging.exception(
                        f"Failed to send notification for schedule {schedule['id']}: {e}"
                    )
                    db.rollback()
        except Exception as e:
            logging.exception("Schedule notification sender failed: %s", e)
        finally:
            db.close()

        await asyncio.sleep(SCHEDULE_NOTIFY_INTERVAL_MINUTES * 60)


@app.get("/api/notifications/schedule-status")
async def get_schedule_notification_status():
    """Check schedule notification system status."""
    db = SessionLocal()
    try:
        upcoming = _get_upcoming_schedules_for_notification(db)
        return {
            "success": True,
            "data": {
                "enabled": SCHEDULE_NOTIFY_ENABLED,
                "notify_hours_before": SCHEDULE_NOTIFY_HOURS_BEFORE,
                "check_interval_minutes": SCHEDULE_NOTIFY_INTERVAL_MINUTES,
                "upcoming_count": len(upcoming),
                "upcoming_schedules": upcoming[:5],
            },
        }
    finally:
        db.close()


@app.post("/api/notifications/trigger-schedule")
async def trigger_schedule_notifications(
    current_user: dict = Depends(get_current_user_record),
):
    """Manually trigger schedule notifications (admin only)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        upcoming = _get_upcoming_schedules_for_notification(db)
        total_sent = 0
        for schedule in upcoming:
            count = _send_schedule_notification(db, schedule)
            total_sent += count

        return {
            "success": True,
            "message": f"Sent {total_sent} schedule notifications",
            "schedules_processed": len(upcoming),
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# Note: Startup/shutdown tasks moved to lifespan context manager
# See: @asynccontextmanager lifespan() near app definition

# ==================== Pydantic request models ====================
import re


def validate_email(email: str) -> str:
    # Basic email format validation
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(email_pattern, email):
        raise ValueError("Invalid email format")
    return email.lower()


def validate_password(password: str) -> str:
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters")
    if len(password) > 128:
        raise ValueError("Password must be less than 128 characters")
    return password


class RegisterRequest(BaseModel):
    email: str
    password: str
    full_name: Optional[str] = None
    barangay: Optional[str] = None
    barangay_id: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    role: Optional[str] = "resident"  # resident, driver, admin

    _validate_email = validator("email", allow_reuse=True)(validate_email)
    _validate_password = validator("password", allow_reuse=True)(validate_password)


class LoginRequest(BaseModel):
    email: str
    password: str

    _validate_email = validator("email", allow_reuse=True)(validate_email)


ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["Authorization", "Content-Type", "X-Requested-With"],
)
def _register_upload_mime_types():
    """Teach `mimetypes` the extensions we serve.

    Slim container images ship without /etc/mime.types, so `.apk` resolves to
    None and Starlette's FileResponse falls back to `text/plain`. Android Chrome
    then sniffs the ZIP container inside the APK and renames the download to
    `*.apk.zip`. Registering the type explicitly fixes that at the source.
    """
    mimetypes.add_type("application/vnd.android.package-archive", ".apk")
    mimetypes.add_type("application/zip", ".zip")
    mimetypes.add_type("application/octet-stream", ".tflite")


_register_upload_mime_types()


class UploadStaticFiles(StaticFiles):
    """StaticFiles that forces a correct, downloadable APK response.

    The mount handles `/uploads/*` before any route declared later, so this is
    the one place that decides the headers for uploaded artifacts.
    """

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if os.path.basename(path).lower().endswith(".apk"):
            response.headers["content-type"] = (
                "application/vnd.android.package-archive"
            )
            # `attachment` keeps mobile browsers from rewriting the filename.
            response.headers["content-disposition"] = (
                f'attachment; filename="{os.path.basename(path)}"'
            )
            response.headers["x-content-type-options"] = "nosniff"
        return response


app.mount("/uploads", UploadStaticFiles(directory=UPLOAD_DIR), name="uploads")

DATABASE_URL = database_url()
if DATABASE_URL is None:
    sqlite_path = os.path.join(DATA_DIR, "test.db")
    bundled_db = os.path.join(os.path.dirname(__file__), "test.db")
    if not os.path.exists(sqlite_path) and os.path.exists(bundled_db):
        import shutil

        try:
            shutil.copy2(bundled_db, sqlite_path)
            print(f"Copied bundled database to {sqlite_path}")
        except Exception as e:
            print(f"DEBUG: Failed to copy bundled database: {e}")
    DATABASE_URL = f"sqlite:///{sqlite_path}"

try:
    if DATABASE_URL.startswith("sqlite:"):
        engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
    else:
        db_pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
        db_max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "5"))
        db_pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "20"))
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=1800,  # Recycle connections every 30 minutes
            pool_size=db_pool_size,
            max_overflow=db_max_overflow,
            pool_timeout=db_pool_timeout,
            connect_args={"connect_timeout": 10},
        )
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
except Exception as e:
    # Never print a connection URL or exception containing credentials.
    print(f"Database engine initialization failed ({type(e).__name__}). Check .env settings.")
    engine = None
    SessionLocal = None


# ============= DATABASE AUTO-SETUP ON STARTUP =============


def run_schema_sync():
    """Reconcile the live schema against database_setup.sql. Always additive.

    Safe to run on every boot and on any database size: it issues at most one
    ALTER per genuinely missing column. A failure here is logged and swallowed
    so the API still starts -- a partially-migrated database is more useful than
    a server that refuses to boot, and the log names the exact statement.
    """
    if not engine or not DATABASE_URL:
        print("Schema: no database configured; skipping reconciliation.")
        return

    sql_path = os.path.join(os.path.dirname(__file__), "database_setup.sql")
    if not os.path.exists(sql_path):
        print(f"[!] Schema: {sql_path} not found; skipping reconciliation.")
        return

    from pathlib import Path as _Path

    db = SessionLocal()
    try:
        report = schema_sync.reconcile(db, _Path(sql_path))
    except Exception as e:
        print(f"[!] Schema reconciliation failed: {e}")
        logging.exception("schema reconciliation failed")
        return
    finally:
        db.close()

    if report.changed:
        print(f"Schema: {report.summary()}")
        for line in report.lines():
            print(line)
        logging.info("schema updated: %s", report.summary())
    else:
        print(f"Schema: {report.summary()}")

    for mismatch in report.type_mismatches:
        # Reported, never auto-altered: changing a live column type can lose or
        # truncate data, so that decision belongs to a human.
        logging.warning("schema type drift (not altered): %s", mismatch)
    if report.type_mismatches:
        print(
            f"Schema: {len(report.type_mismatches)} column type(s) differ from "
            "database_setup.sql (left unchanged; see log)"
        )
    for error in report.errors:
        print(f"[!] Schema error: {error}")
        logging.error("schema error: %s", error)


def run_sql_setup_script():
    """Execute database_setup.sql against MySQL using a raw connection.

    Skipped when using SQLite since the script contains MySQL-specific constructs
    (DELIMITER directives, stored procedures, ON DUPLICATE KEY UPDATE, etc.).
    """
    # Skip for SQLite - the SQL file contains MySQL-specific syntax
    db_url = DATABASE_URL or ""
    if db_url.startswith("sqlite") or not db_url:
        print("run_sql_setup_script: skipping (SQLite or no URL)")
        return

    if not engine:
        print("[!] run_sql_setup_script: no database engine available, skipping.")
        return

    sql_path = os.path.join(os.path.dirname(__file__), "database_setup.sql")
    if not os.path.exists(sql_path):
        print(f"[!] run_sql_setup_script: {sql_path} not found, skipping.")
        return

    with open(sql_path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    # Strip DELIMITER directives and normalise statement separators.
    # The file uses:
    #   DELIMITER //
    #   ...body...
    #   END//
    #   DELIMITER ;
    # We replace the custom delimiter back to ";" so we can split on ";".
    import re

    # Remove DELIMITER lines and replace the custom-delimiter terminator (//)
    # with a plain semicolon so the whole file can be split on ";".
    processed = re.sub(r"DELIMITER\s+//", "", raw_sql)
    processed = re.sub(r"DELIMITER\s+;", "", processed)
    # Replace "//" end-of-block markers with ";"
    processed = re.sub(r"//", ";", processed)

    # Split on ";" and filter out empty / comment-only / non-DML statements
    # that would fail when sent over the wire (SHOW TABLES, SELECT 'status').
    skip_prefixes = ("show ", "select '", "use ")
    statements = []
    for raw_stmt in processed.split(";"):
        stmt = raw_stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        # Drop lines that are purely comments
        non_comment_lines = [
            ln
            for ln in stmt.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        ]
        if not non_comment_lines:
            continue
        first_word = non_comment_lines[0].strip().lower()
        if any(first_word.startswith(p) for p in skip_prefixes):
            continue
        statements.append(stmt)

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        executed = 0
        errors = 0
        for stmt in statements:
            try:
                cursor.execute(stmt)
                conn.commit()
                executed += 1
            except Exception as stmt_err:
                # Ignore "already exists" / "duplicate column" errors — the
                # script is designed to be idempotent via IF NOT EXISTS.
                err_msg = str(stmt_err).lower()
                if any(
                    k in err_msg
                    for k in ("already exists", "duplicate column", "duplicate key")
                ):
                    conn.commit()
                    executed += 1
                else:
                    logging.warning(
                        "SQL setup statement skipped (%s): %.120s", stmt_err, stmt
                    )
                    errors += 1
        cursor.close()
        print(
            f"[OK] database_setup.sql executed: {executed} statements OK, {errors} skipped."
        )
    except Exception as e:
        logging.error("run_sql_setup_script failed: %s", e)
        print(f"[!] database_setup.sql execution error: {e}")
    finally:
        conn.close()


# Columns the request handlers rely on. Checked on every startup (cheap
# information_schema lookups) because setup_database() is off by default and
# its "ADD COLUMN IF NOT EXISTS" is MariaDB-only syntax that MySQL rejects.
_REQUIRED_COLUMNS = {
    "announcements": {
        "is_active": "BOOLEAN DEFAULT TRUE",
        "is_urgent": "BOOLEAN DEFAULT FALSE",
        "target_role": "VARCHAR(20) DEFAULT 'all'",
        "target_barangay": "VARCHAR(100) DEFAULT NULL",
        "created_by": "VARCHAR(36)",
    },
    "collection_records": {
        "skip_reason": "TEXT",
        "skipped_at": "TIMESTAMP NULL",
    },
    "notifications": {
        "target_scope": "VARCHAR(20) DEFAULT NULL",
        "target_barangay": "VARCHAR(100) DEFAULT NULL",
        "target_role": "VARCHAR(20) DEFAULT NULL",
        "schedule_id": "VARCHAR(36) DEFAULT NULL",
    },
    # /api/auth/create-user and profile updates write phone, but the original
    # schema never declared it, so those requests failed with a 500.
    "users": {
        "phone": "VARCHAR(20) DEFAULT NULL",
    },
    # `status` is liveness (the sweeper rewrites it from last_updated), so it
    # cannot also record the driver's toggle intent. Without a separate column
    # the truck flipped offline while the driver was still on duty.
    "truck_locations": {
        "is_on_duty": "BOOLEAN DEFAULT FALSE",
    },
}


# Columns that must accept NULL even though the original CREATE TABLE marked them
# NOT NULL. notifications.user_id is NULL for broadcast rows (announcements and
# schedule reminders target a scope, not one user), so a NOT NULL constraint makes
# every broadcast insert fail. SQLite cannot drop a constraint in place, so it is
# rebuilt; MySQL is altered directly.
_NULLABLE_OVERRIDES = {
    "notifications": {"user_id": "VARCHAR(36)"},
}


def _relax_notification_user_id(db) -> None:
    """Make notifications.user_id nullable without losing data."""
    if _is_sqlite(db):
        info = db.execute(text("PRAGMA table_info(notifications)")).fetchall()
        if not info:
            return
        by_name = {row[1]: row for row in info}
        user_col = by_name.get("user_id")
        if user_col is None or user_col[3] == 0:
            return  # already nullable

        existing = [row[1] for row in info]
        columns = ", ".join(
            f"{name} {by_name[name][2]}"
            + (" PRIMARY KEY" if by_name[name][5] else "")
            for name in existing
        )
        # Rebuild the table: SQLite has no ALTER COLUMN.
        db.execute(text("PRAGMA foreign_keys=OFF"))
        db.execute(text(f"CREATE TABLE notifications_rebuild ({columns})"))
        col_list = ", ".join(existing)
        db.execute(
            text(
                f"INSERT INTO notifications_rebuild ({col_list}) "
                f"SELECT {col_list} FROM notifications"
            )
        )
        db.execute(text("DROP TABLE notifications"))
        db.execute(text("ALTER TABLE notifications_rebuild RENAME TO notifications"))
        schema_sync.create_index_if_missing(
            db, "notifications", "idx_notifications_user_id", "user_id",
        )
        db.commit()
        logging.info("Rebuilt notifications table so user_id accepts NULL")
        return

    row = db.execute(
        text(
            "SELECT IS_NULLABLE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notifications' "
            "AND COLUMN_NAME = 'user_id'"
        )
    ).fetchone()
    if row is None or str(row[0]).upper() == "YES":
        return
    db.execute(text("ALTER TABLE notifications MODIFY user_id VARCHAR(36) NULL"))
    db.commit()
    logging.info("Altered notifications.user_id to accept NULL")


def ensure_required_columns():
    """Add any missing columns from _REQUIRED_COLUMNS. Never drops or alters."""
    if not engine or not DATABASE_URL:
        return
    db = SessionLocal()
    try:
        if _is_sqlite(db):
            for table, columns in _REQUIRED_COLUMNS.items():
                existing = {
                    row[1]
                    for row in db.execute(
                        text(f"PRAGMA table_info({table})")
                    ).fetchall()
                }
                if not existing:
                    continue  # table missing entirely; setup_database() owns that
                for column, ddl in columns.items():
                    if column not in existing:
                        db.execute(
                            text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
                        )
                        logging.info("Added missing column %s.%s", table, column)
            db.commit()
            # Broadcast notification rows need user_id to be nullable.
            _relax_notification_user_id(db)
            # Trucks already marked active were on duty before is_on_duty
            # existed; adopt them so the sweeper doesn't end their shift.
            db.execute(
                text(
                    "UPDATE truck_locations SET is_on_duty = TRUE "
                    "WHERE is_on_duty = FALSE AND status = 'active'"
                )
            )
            db.commit()
            return

        for table, columns in _REQUIRED_COLUMNS.items():
            existing = {
                row[0]
                for row in db.execute(
                    text(
                        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t"
                    ),
                    {"t": table},
                ).fetchall()
            }
            if not existing:
                continue  # table missing entirely; setup_database() owns that
            for column, ddl in columns.items():
                if column not in existing:
                    db.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
                    logging.info("Added missing column %s.%s", table, column)
        db.commit()
        # Broadcast notification rows need user_id to be nullable.
        _relax_notification_user_id(db)
    except Exception as e:
        db.rollback()
        print(f"⚠️ ensure_required_columns failed: {e}")
    finally:
        db.close()


def setup_database():
    """Create missing tables if they don't exist."""
    if not engine:
        print("Warning: No database engine for setup.")
        return

    db = SessionLocal()
    try:
        # Schedules Table (primary table missing from original setup)
        # SQLite-compatible: no ON UPDATE CURRENT_TIMESTAMP, no inline INDEX, no COMMENT
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS schedules (
                id VARCHAR(36) PRIMARY KEY,
                barangay VARCHAR(100) NOT NULL,
                waste_type VARCHAR(100),
                time_slot VARCHAR(20),
                day_of_week INT,
                collection_type VARCHAR(50) DEFAULT 'all',
                start_time VARCHAR(20),
                end_time VARCHAR(20),
                driver_id VARCHAR(36),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )
        schema_sync.create_index_if_missing(db, "schedules", "idx_sched_barangay", "barangay")
        schema_sync.create_index_if_missing(db, "schedules", "idx_sched_day", "day_of_week")
        schema_sync.create_index_if_missing(db, "schedules", "idx_sched_driver", "driver_id")

        # Collection Records Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS collection_records (
                id VARCHAR(36) PRIMARY KEY,
                driver_id VARCHAR(36),
                schedule_id VARCHAR(36),
                barangay VARCHAR(100),
                waste_type VARCHAR(50),
                time_slot VARCHAR(50),
                date DATE,
                status VARCHAR(20) DEFAULT 'pending',
                waste_amount_kg DECIMAL(10,2) DEFAULT 0,
                biodegradable_kg DECIMAL(10,2) DEFAULT 0,
                non_biodegradable_kg DECIMAL(10,2) DEFAULT 0,
                recyclable_kg DECIMAL(10,2) DEFAULT 0,
                special_waste_kg DECIMAL(10,2) DEFAULT 0,
                notes TEXT,
                collected_at TIMESTAMP NULL,
                skipped_at TIMESTAMP NULL,
                skip_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Truck Locations Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS truck_locations (
                id VARCHAR(36) PRIMARY KEY,
                truck_id VARCHAR(36) NOT NULL,
                driver_name VARCHAR(255),
                plate_number VARCHAR(20),
                latitude DECIMAL(10,8) NOT NULL,
                longitude DECIMAL(11,8) NOT NULL,
                current_barangay VARCHAR(100),
                status VARCHAR(20) DEFAULT 'active',
                speed DECIMAL(6,2),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Rewards Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS rewards (
                id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                points_required INT NOT NULL,
                image_url VARCHAR(500),
                available_quantity INT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Redemptions Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS redemptions (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36) NOT NULL,
                reward_id VARCHAR(36) NOT NULL,
                reward_name VARCHAR(100),
                points_spent INT NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                redeemed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Surveys Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS surveys (
                id VARCHAR(36) PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                description TEXT,
                questions JSON,
                target_barangay VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Survey Responses Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS survey_responses (
                id VARCHAR(36) PRIMARY KEY,
                survey_id VARCHAR(36) NOT NULL,
                user_id VARCHAR(36) NOT NULL,
                responses JSON NOT NULL,
                submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Notifications Table
        # user_id is NULL for broadcast notifications (target_scope determines audience).
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS notifications (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36),
                title VARCHAR(255) NOT NULL,
                body TEXT NOT NULL,
                type VARCHAR(20) DEFAULT 'system',
                is_read BOOLEAN DEFAULT FALSE,
                target_scope VARCHAR(20) DEFAULT NULL,
                target_barangay VARCHAR(100) DEFAULT NULL,
                target_role VARCHAR(20) DEFAULT NULL,
                schedule_id VARCHAR(36) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Tracks which broadcast notifications a user has read or dismissed.
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS notification_reads (
                id VARCHAR(36) PRIMARY KEY,
                notification_id VARCHAR(36) NOT NULL,
                user_id VARCHAR(36) NOT NULL,
                is_read BOOLEAN DEFAULT TRUE,
                dismissed BOOLEAN DEFAULT FALSE,
                read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )
        schema_sync.create_index_if_missing(
            db, "notification_reads", "idx_notif_reads_unique",
            "notification_id, user_id", unique=True,
        )
        schema_sync.create_index_if_missing(
            db, "notifications", "idx_notif_target_scope", "target_scope",
        )

        # Users Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS users (
                id VARCHAR(36) PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                full_name VARCHAR(100),
                barangay VARCHAR(100),
                role VARCHAR(20) DEFAULT 'resident',
                phone VARCHAR(20),
                total_points INT DEFAULT 0,
                lifetime_points INT DEFAULT 0,
                reports_submitted INT DEFAULT 0,
                assigned_truck_plate VARCHAR(20),
                profile_image_url VARCHAR(500),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Reports Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS reports (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36),
                barangay VARCHAR(100),
                description TEXT NOT NULL,
                category VARCHAR(50),
                status VARCHAR(20) DEFAULT 'pending',
                points_awarded INT DEFAULT 0,
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                address VARCHAR(255),
                image_url VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Barangays Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS barangays (
                id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                district VARCHAR(50),
                latitude DOUBLE,
                longitude DOUBLE,
                population INT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Announcements Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS announcements (
                id VARCHAR(36) PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                is_urgent BOOLEAN DEFAULT FALSE,
                target_role VARCHAR(20) DEFAULT 'all',
                target_barangay VARCHAR(100) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by VARCHAR(36)
            )
        """)
        )

        # Add missing columns to announcements table if they don't exist
        try:
            db.execute(
                text("ALTER TABLE announcements ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE")
            )
        except:
            pass
        try:
            db.execute(
                text("ALTER TABLE announcements ADD COLUMN IF NOT EXISTS is_urgent BOOLEAN DEFAULT FALSE")
            )
        except:
            pass
        try:
            db.execute(
                text("ALTER TABLE announcements ADD COLUMN IF NOT EXISTS target_role VARCHAR(20) DEFAULT 'all'")
            )
        except:
            pass
        try:
            db.execute(
                text("ALTER TABLE announcements ADD COLUMN IF NOT EXISTS target_barangay VARCHAR(100) DEFAULT NULL")
            )
        except:
            pass
        try:
            db.execute(
                text("ALTER TABLE announcements ADD COLUMN IF NOT EXISTS created_by VARCHAR(36)")
            )
        except:
            pass

        # Add missing columns to schedules table if they don't exist
        try:
            db.execute(
                text("ALTER TABLE schedules ADD COLUMN IF NOT EXISTS day_of_week INT")
            )
        except:
            pass  # Column might already exist

        try:
            db.execute(
                text(
                    "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS collection_type VARCHAR(50) DEFAULT 'all'"
                )
            )
        except:
            pass

        try:
            db.execute(
                text(
                    "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS start_time VARCHAR(20)"
                )
            )
        except:
            pass

        try:
            db.execute(
                text(
                    "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS end_time VARCHAR(20)"
                )
            )
        except:
            pass

        try:
            db.execute(
                text(
                    "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS driver_id VARCHAR(36)"
                )
            )
        except:
            pass

        try:
            db.execute(
                text(
                    "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE"
                )
            )
        except:
            pass

        # Add missing columns to users table
        try:
            db.execute(text("ALTER TABLE users ADD COLUMN total_points INT DEFAULT 0"))
        except:
            pass

        try:
            db.execute(
                text("ALTER TABLE users ADD COLUMN lifetime_points INT DEFAULT 0")
            )
        except:
            pass

        try:
            db.execute(
                text("ALTER TABLE users ADD COLUMN reports_submitted INT DEFAULT 0")
            )
        except:
            pass

        try:
            db.execute(
                text("ALTER TABLE users ADD COLUMN assigned_truck_plate VARCHAR(20)")
            )
        except:
            pass

        try:
            db.execute(
                text("ALTER TABLE users ADD COLUMN profile_image_url VARCHAR(500)")
            )
        except:
            pass

        try:
            db.execute(
                text("ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
            )
        except:
            pass

        # Add missing columns to reports table
        try:
            db.execute(text("ALTER TABLE reports ADD COLUMN latitude DECIMAL(10, 8)"))
        except:
            pass

        try:
            db.execute(text("ALTER TABLE reports ADD COLUMN longitude DECIMAL(11, 8)"))
        except:
            pass

        try:
            db.execute(text("ALTER TABLE reports ADD COLUMN address VARCHAR(255)"))
        except:
            pass

        try:
            db.execute(text("ALTER TABLE reports ADD COLUMN image_url VARCHAR(500)"))
        except:
            pass

        try:
            db.execute(text("ALTER TABLE rewards ADD COLUMN image_url VARCHAR(500)"))
        except:
            pass

        try:
            db.execute(text("ALTER TABLE rewards ADD COLUMN available_quantity INT"))
        except:
            pass

        try:
            schema_sync.create_index_if_missing(db, "collection_records", "idx_date", "date")
        except:
            pass

        # App Update Table
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS app_update (
                id VARCHAR(36) PRIMARY KEY,
                latest_version VARCHAR(20) NOT NULL DEFAULT '',
                update_url TEXT NOT NULL,
                force_update BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        # Scanner correction log.
        #
        # Previously created only inside the /api/waste-scan/correction handler,
        # so it did not exist until someone submitted a correction. Any schema
        # inspection (health check, admin DB view, our own migration report)
        # therefore saw a database that was missing a table, and the first
        # correction request had to create it mid-request. Creating it at startup
        # keeps the schema complete and the handler cheap.
        db.execute(
            text("""
            CREATE TABLE IF NOT EXISTS scan_corrections (
                id VARCHAR(36) PRIMARY KEY,
                scan_id VARCHAR(36) NULL,
                user_id VARCHAR(36) NOT NULL,
                predicted_category VARCHAR(100),
                corrected_category VARCHAR(100) NOT NULL,
                confidence FLOAT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )

        db.commit()
        print("[OK] Database tables verified/created successfully!")
    except Exception as e:
        # One failing statement must not cancel the tables declared after it.
        # This used to abort the whole function on a fresh MySQL database (the
        # non-portable CREATE INDEX above raised 1064), leaving users, reports
        # and everything declared later permanently missing.
        print(f"[!] Database setup warning: {str(e).splitlines()[0][:200]}")
        db.rollback()
    finally:
        db.close()


# ============= MIGRATIONS =============


def _ensure_migrations_table():
    """Create the migration ledger without applying anything.

    run_migrations() needs this table to record which scripts ran, and the
    schema contract expects it to exist whether or not any .sql migrations
    were shipped. Kept separate so the early-return path can create it too.
    """
    if not engine:
        return
    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    finally:
        conn.close()


def run_migrations():
    """Run schema migration scripts from the migrations/ directory.

    Each migration runs once tracked in a _migrations table.
    MySQL only — skipped for SQLite.
    """
    db_url = DATABASE_URL or ""
    if db_url.startswith("sqlite") or not db_url:
        return

    if not engine:
        return

    migrations_dir = os.path.join(os.path.dirname(__file__), "migrations")
    if not os.path.isdir(migrations_dir):
        # The folder ships empty or absent (the spec bundles no .sql files yet),
        # so there is nothing to apply -- but returning here used to skip the
        # _migrations table too. That table is part of the schema the app expects,
        # and its absence made health checks report the database as incomplete.
        # Create it before returning so the schema is consistent either way.
        _ensure_migrations_table()
        return

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()

        # Create migration tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS _migrations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL UNIQUE,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # Get already-applied migrations
        cursor.execute("SELECT filename FROM _migrations")
        applied = {row[0] for row in cursor.fetchall()}

        # Find .sql files sorted by name
        sql_files = sorted(
            f for f in os.listdir(migrations_dir)
            if f.endswith(".sql") and f not in applied
        )

        for filename in sql_files:
            filepath = os.path.join(migrations_dir, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                raw_sql = f.read()

            # Strip SQL comments and split on semicolons
            import re
            statements = []
            for raw_stmt in raw_sql.split(";"):
                stmt = raw_stmt.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                non_comment = [
                    ln for ln in stmt.splitlines()
                    if ln.strip() and not ln.strip().startswith("--")
                ]
                if not non_comment:
                    continue
                statements.append(stmt)

            executed = 0
            errors = 0
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    conn.commit()
                    executed += 1
                except Exception as e:
                    err_msg = str(e).lower()
                    if any(k in err_msg for k in ("already exists", "duplicate column", "duplicate key", "duplicate entry")):
                        conn.commit()
                        executed += 1
                    else:
                        logging.warning("Migration %s stmt skipped: %s", filename, str(e)[:120])
                        errors += 1

            # Record migration as applied
            cursor.execute(
                "INSERT INTO _migrations (filename) VALUES (%s)",
                (filename,)
            )
            conn.commit()
            print(f"[OK] Migration {filename}: {executed} OK, {errors} skipped")

        cursor.close()
    except Exception as e:
        logging.error("run_migrations failed: %s", e)
        print(f"[!] Migration error: {e}")
    finally:
        conn.close()


# ============= DATABASE INIT ON STARTUP =============
# Note: Database and mDNS initialization moved to lifespan context manager
# See: @asynccontextmanager lifespan() near app definition


async def register_mdns():
    """Advertise the configured port even with no Internet connection."""
    await advertise_server()


# ============= MODELS =============


class TruckLocation(BaseModel):
    truck_id: str
    driver_name: Optional[str] = None
    plate_number: Optional[str] = None
    latitude: float
    longitude: float
    current_barangay: Optional[str] = None
    status: str = "active"
    speed: Optional[float] = None


class NotificationCreate(BaseModel):
    user_id: str
    title: str
    body: str
    type: str = "system"


class ReportCreate(BaseModel):
    user_id: Optional[str] = None  # Optional - will use auth token if not provided
    barangay: Optional[str] = None
    barangay_id: Optional[str] = None  # Flutter sends this
    description: Optional[str] = None
    category: Optional[str] = None
    report_type: Optional[str] = None  # Flutter sends this
    title: Optional[str] = None  # Sometimes sent
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None


class ScheduleCreate(BaseModel):
    barangay: str
    day_of_week: int
    waste_type: str
    time_slot: str
    driver_id: Optional[str] = None
    route_name: str = ""
    collection_points: List[dict] = []

    _route_name = validator("route_name", pre=True, allow_reuse=True)(validate_route_name)
    _points = validator("collection_points", pre=True, allow_reuse=True)(validate_points)


# ============= TRACKING =============


@app.post("/api/tracking/update")
async def update_tracking(
    location: TruckLocation, current_user: dict = Depends(get_current_user_record)
):
    """Update truck location. Requires driver role authentication."""
    user_role = current_user.get("role", "resident")
    if user_role not in ("driver", "admin"):
        raise HTTPException(
            status_code=403, detail="Only drivers can update truck locations"
        )

    db = SessionLocal()
    try:
        # Always use the authenticated user's full UUID as truck_id
        # This ensures it matches users.id for JOIN queries
        clean_truck_id = current_user.get("id")

        # Use truck_id as the unique key for upsert (not a fresh UUID)
        existing = db.execute(
            text("SELECT id FROM truck_locations WHERE truck_id = :truck_id LIMIT 1"),
            {"truck_id": clean_truck_id},
        ).fetchone()

        row_id = existing[0] if existing else str(uuid.uuid4())
        if not existing and clean_truck_id:
            # Reuse a legacy DRIVER_<first8> row so it is migrated instead of
            # leaving a second, ghost truck behind.
            legacy = db.execute(
                text("SELECT id FROM truck_locations WHERE truck_id = :legacy LIMIT 1"),
                {"legacy": f"DRIVER_{str(clean_truck_id)[:8]}"},
            ).fetchone()
            if legacy:
                row_id = legacy[0]

        truck_values = {
            "id": row_id,
            "truck_id": clean_truck_id,
            "driver_name": location.driver_name or current_user.get("full_name"),
            "plate_number": location.plate_number,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "current_barangay": location.current_barangay,
            "status": location.status,
            "speed": location.speed,
        }
        # Duty state (status) is owned by PUT /api/driver/status and
        # /api/driver/go-offline. The periodic GPS ping must not touch it, or it
        # overwrites the driver's toggle every 30s and the truck never stays
        # online. The client always sends status='active', so honoring it here
        # would also resurrect a truck the driver just put offline.
        if _is_sqlite(db):
            db.execute(
                text("""
                    INSERT INTO truck_locations
                       (id, truck_id, driver_name, plate_number, latitude, longitude,
                        current_barangay, status, speed, last_updated)
                    VALUES (:id, :truck_id, :driver_name, :plate_number,
                            :latitude, :longitude, :current_barangay, :status, :speed, CURRENT_TIMESTAMP)
                    ON CONFLICT(id) DO UPDATE SET
                        truck_id         = excluded.truck_id,
                        driver_name      = excluded.driver_name,
                        plate_number     = excluded.plate_number,
                        latitude         = excluded.latitude,
                        longitude        = excluded.longitude,
                        current_barangay = excluded.current_barangay,
                        speed            = excluded.speed,
                        last_updated     = CURRENT_TIMESTAMP
                """),
                truck_values,
            )
        else:
            db.execute(
                text("""
                    INSERT INTO truck_locations
                       (id, truck_id, driver_name, plate_number, latitude, longitude,
                        current_barangay, status, speed, last_updated)
                    VALUES (:id, :truck_id, :driver_name, :plate_number,
                            :latitude, :longitude, :current_barangay, :status, :speed, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                        truck_id         = VALUES(truck_id),
                        driver_name      = VALUES(driver_name),
                        plate_number     = VALUES(plate_number),
                        latitude         = VALUES(latitude),
                        longitude        = VALUES(longitude),
                        current_barangay = VALUES(current_barangay),
                        speed            = VALUES(speed),
                        last_updated     = CURRENT_TIMESTAMP
                """),
                truck_values,
            )
        db.commit()
        return {"status": "success"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/trucks/locations")
async def get_truck_locations(status: str = None, limit: int = 200, offset: int = 0, response: Response = None):
    """Get truck locations. Filters by recency (5 min) by default."""
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=200, max_limit=300)
        cutoff = get_ph_time().replace(tzinfo=None) - timedelta(minutes=5)
        cache_key = f"{status}|{limit}|{offset}"
        cached_locations = _cache_get(_truck_locations_cache, cache_key, TRUCK_LOCATIONS_CACHE_SECONDS)
        if cached_locations is not None:
            if response:
                response.headers["X-Cache"] = "HIT"
            return {"data": cached_locations}

        driver_match = _driver_truck_match_sql(db)
        # tl.* would hand back a naive local-time last_updated. Select the real
        # UTC instant separately; the mapping below prefers it.
        updated_col = truck_last_updated_column(db.get_bind().dialect.name)
        if status and status != "all":
            result = db.execute(
                text(f"""SELECT tl.*, u.id AS driver_user_id, {updated_col}
                    FROM truck_locations tl
                    INNER JOIN users u ON u.role = 'driver' AND {driver_match}
                    WHERE (tl.last_updated >= :cutoff OR tl.is_on_duty = TRUE)
                    AND NOT (tl.latitude = 0 AND tl.longitude = 0)
                    AND tl.status = :status
                    ORDER BY tl.last_updated DESC LIMIT :limit OFFSET :offset"""),
                {"status": status, "limit": limit, "offset": offset, "cutoff": cutoff},
            ).fetchall()
        else:
            result = db.execute(
                text(f"""SELECT tl.*, u.id AS driver_user_id, {updated_col}
                    FROM truck_locations tl
                    INNER JOIN users u ON u.role = 'driver' AND {driver_match}
                    WHERE (tl.last_updated >= :cutoff OR tl.is_on_duty = TRUE)
                    AND NOT (tl.latitude = 0 AND tl.longitude = 0)
                    ORDER BY tl.last_updated DESC LIMIT :limit OFFSET :offset"""),
                {"limit": limit, "offset": offset, "cutoff": cutoff},
            ).fetchall()

        locations_by_driver = {}
        for row in result:
            mapping = row._mapping
            key = mapping.get("driver_user_id") or mapping.get("truck_id")
            if key not in locations_by_driver:
                locations_by_driver[key] = _serialize_truck_location(row)
        locations = list(locations_by_driver.values())
        _cache_set(_truck_locations_cache, cache_key, locations)
        if response:
            response.headers["X-Cache"] = "MISS"
        return {"data": locations}
    finally:
        db.close()


@app.get("/api/fleet/live")
async def get_live_fleet(response: Response, current_user: dict = Depends(get_current_user_record)):
    """Get trucks currently on active schedules today with recent GPS updates. Admin only."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    if FLEET_LIVE_CACHE_SECONDS > 0:
        cached_data = _fleet_live_cache.get("data")
        cached_ts = _fleet_live_cache.get("timestamp")
        if cached_data is not None and cached_ts is not None:
            if (datetime.now() - cached_ts).total_seconds() < FLEET_LIVE_CACHE_SECONDS:
                response.headers["X-Cache"] = "HIT"
                return {"success": True, "data": cached_data}

    db = SessionLocal()
    try:
        today = get_ph_weekday()
        max_rows = 200
        cutoff = get_ph_time().replace(tzinfo=None) - timedelta(minutes=5)
        driver_match = _driver_truck_match_sql(db)
        result = db.execute(
            text(f"""
                SELECT DISTINCT
                    u.id AS driver_user_id,
                    tl.truck_id, tl.driver_name, tl.plate_number,
                    tl.latitude, tl.longitude, tl.current_barangay,
                    tl.status, tl.speed, tl.is_on_duty,
                    {truck_last_updated_column(db.get_bind().dialect.name)},
                    s.barangay as schedule_barangay,
                    s.start_time, s.end_time,
                    s.collection_type as waste_type
                FROM truck_locations tl
                INNER JOIN users u ON u.role = 'driver'
                    AND {driver_match}
                LEFT JOIN schedules s ON s.driver_id = u.id
                    AND s.day_of_week = :today
                    AND s.is_active = TRUE
                WHERE (tl.last_updated >= :cutoff OR tl.is_on_duty = TRUE)
                    AND NOT (tl.latitude = 0 AND tl.longitude = 0)
                    AND tl.status = 'active'
                -- With DISTINCT, MySQL tolerates ordering by a column that is
                -- not selected; TiDB rejects it (ERROR 3065). Sort by the
                -- selected epoch alias instead so both engines agree.
                ORDER BY last_updated DESC
                LIMIT :limit
            """),
            {"today": today, "limit": max_rows, "cutoff": cutoff},
        ).fetchall()

        trucks = []
        seen_fleet = set()
        for row in result:
            mapping = row._mapping
            fleet_key = (
                mapping.get("driver_user_id") or mapping.get("truck_id"),
                mapping.get("schedule_barangay"),
                mapping.get("start_time"),
                mapping.get("end_time"),
            )
            if fleet_key in seen_fleet:
                continue
            seen_fleet.add(fleet_key)
            last_updated = mapping.get("last_updated")
            trucks.append({
                "truck_id": mapping.get("truck_id"),
                "driver_name": mapping.get("driver_name"),
                "plate_number": mapping.get("plate_number"),
                "latitude": float(mapping.get("latitude") or 0),
                "longitude": float(mapping.get("longitude") or 0),
                "current_barangay": mapping.get("current_barangay"),
                # Judge staleness the same way /api/trucks does, so the fleet
                # view and the truck list never disagree about a truck.
                "status": _effective_truck_status(
                    mapping.get("status"),
                    last_updated,
                    mapping.get("is_on_duty"),
                ),
                "speed": float(mapping.get("speed") or 0),
                "last_updated": format_datetime_utc(last_updated),
                "schedule_barangay": mapping.get("schedule_barangay"),
                "time_slot": (
                    f"{mapping.get('start_time') or ''} - "
                    f"{mapping.get('end_time') or ''}".strip(" -")
                ),
                "waste_type": mapping.get("waste_type"),
            })
        if FLEET_LIVE_CACHE_SECONDS > 0:
            _fleet_live_cache["data"] = trucks
            _fleet_live_cache["timestamp"] = datetime.now()
        response.headers["X-Cache"] = "MISS"
        return {"success": True, "data": trucks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/trucks/{driver_id}")
async def update_truck_status(driver_id: str, request: dict, current_user: dict = Depends(get_current_user_record)):
    """Update truck status."""
    db = SessionLocal()
    try:
        status = request.get("status", "active")
        db.execute(
            text(
                "UPDATE truck_locations SET status = :status WHERE truck_id = :driver_id"
            ),
            {"status": status, "driver_id": driver_id},
        )
        db.commit()
        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/trucks")
async def get_trucks(barangay: str = None, limit: int = 200, offset: int = 0):
    """List current assigned vehicles, not historical driver tracking records."""
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=200, max_limit=300)
        # Driver registrations own the current name/plate. Retain tracking
        # history but join only the latest location for each assigned driver.
        # LEFT JOIN keeps an assigned vehicle visible before its first GPS fix.
        query = f"""
            SELECT COALESCE(tl.id, u.id) AS id, u.id AS truck_id,
                   u.full_name AS driver_name,
                   TRIM(u.assigned_truck_plate) AS plate_number,
                   tl.latitude, tl.longitude, tl.current_barangay,
                   COALESCE(tl.status, 'offline') AS status,
                   COALESCE(tl.speed, 0) AS speed,
                   tl.is_on_duty,
                   {truck_last_updated_column(db.get_bind().dialect.name)}
            FROM users u
            -- Latest row per truck. A correlated subquery inside ON is
            -- rejected by TiDB ("ON condition doesn't support subqueries yet")
            -- and forces one lookup per driver; ranking once is portable to
            -- MySQL 8, MariaDB 10.2+, SQLite 3.25+ and TiDB.
            LEFT JOIN (
                SELECT * FROM (
                    SELECT latest.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY latest.truck_id
                               ORDER BY latest.last_updated DESC, latest.id DESC
                           ) AS _latest_rank
                    FROM truck_locations latest
                ) ranked
                WHERE ranked._latest_rank = 1
            ) tl ON tl.truck_id = u.id
            WHERE u.role = 'driver'
              AND (u.is_active IS NULL OR u.is_active = TRUE)
              AND u.assigned_truck_plate IS NOT NULL
              AND TRIM(u.assigned_truck_plate) <> ''
              AND UPPER(TRIM(u.assigned_truck_plate)) NOT IN ('UNASSIGNED', 'PENDING')
        """
        params = {"limit": limit, "offset": offset}
        if barangay:
            query += " AND tl.current_barangay = :barangay"
            params["barangay"] = barangay
        query += " ORDER BY tl.last_updated DESC, u.id ASC LIMIT :limit OFFSET :offset"
        result = db.execute(text(query), params).fetchall()

        trucks = [_serialize_truck_location(row) for row in result]
        return {"success": True, "data": trucks}
    finally:
        db.close()


@app.post("/api/trucks/locations/offline")
async def set_truck_offline(request: dict, current_user: dict = Depends(get_current_user_record)):
    """Set truck status to offline when driver stops tracking."""
    db = SessionLocal()
    try:
        truck_id = request.get("truck_id") or request.get("truckId")
        db.execute(
            text(
                "UPDATE truck_locations SET status = 'offline' WHERE truck_id = :truck_id"
            ),
            {"truck_id": truck_id},
        )
        db.commit()
        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# --------------- Broadcast notification helpers ---------------

_notification_reads_table_ready = False


def _ensure_notification_reads_table(db):
    """Create the notification_reads table once per process."""
    global _notification_reads_table_ready
    if _notification_reads_table_ready:
        return
    db.execute(
        text("""
            CREATE TABLE IF NOT EXISTS notification_reads (
                id VARCHAR(36) PRIMARY KEY,
                notification_id VARCHAR(36) NOT NULL,
                user_id VARCHAR(36) NOT NULL,
                is_read BOOLEAN DEFAULT TRUE,
                dismissed BOOLEAN DEFAULT FALSE,
                read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    )
    if _is_sqlite(db):
        db.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_reads_unique "
            "ON notification_reads(notification_id, user_id)"
        ))
    else:
        # MySQL: check if the index already exists before creating
        existing = db.execute(text(
            "SELECT COUNT(*) FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notification_reads' "
            "AND INDEX_NAME = 'idx_notif_reads_unique'"
        )).fetchone()[0]
        if not existing:
            db.execute(text(
                "CREATE UNIQUE INDEX idx_notif_reads_unique "
                "ON notification_reads(notification_id, user_id)"
            ))
    db.commit()
    _notification_reads_table_ready = True


def _upsert_notification_read(db, notification_id, user_id, *, is_read=None, dismissed=None):
    """Insert or update a notification_reads row for a broadcast notification."""
    existing = db.execute(
        text(
            "SELECT id, is_read, dismissed FROM notification_reads "
            "WHERE notification_id = :nid AND user_id = :uid"
        ),
        {"nid": notification_id, "uid": user_id},
    ).fetchone()

    if existing:
        updates = []
        params = {"nid": notification_id, "uid": user_id}
        if is_read is not None:
            updates.append("is_read = :is_read")
            params["is_read"] = is_read
        if dismissed is not None:
            updates.append("dismissed = :dismissed")
            params["dismissed"] = dismissed
        if updates:
            updates.append("read_at = CURRENT_TIMESTAMP")
            db.execute(
                text(
                    f"UPDATE notification_reads SET {', '.join(updates)} "
                    "WHERE notification_id = :nid AND user_id = :uid"
                ),
                params,
            )
    else:
        db.execute(
            text("""
                INSERT INTO notification_reads
                    (id, notification_id, user_id, is_read, dismissed, read_at)
                VALUES (:id, :nid, :uid, :is_read, :dismissed, CURRENT_TIMESTAMP)
            """),
            {
                "id": str(uuid.uuid4()),
                "nid": notification_id,
                "uid": user_id,
                "is_read": is_read if is_read is not None else False,
                "dismissed": dismissed if dismissed is not None else False,
            },
        )


@app.post("/api/notifications")
async def create_notification(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Create and send notification to users.

    Broadcast notifications (target='all' or target='barangay') store a single
    row with target_scope set. Every matching user sees it at read time, including
    users who register after the notification was created.

    Request body:
    - title: str - notification title
    - body: str - notification body
    - type: str - notification type (optional, default: 'general')
    - target: str - 'all', 'barangay', or 'user' (optional, default: 'all')
    - barangay: str - filter by barangay (required if target='barangay')
    - user_id: str - send to specific user (required if target='user')
    """
    db = SessionLocal()
    try:
        title = request.get("title", "")
        body = request.get("body", "")
        notification_type = request.get("type", "general")
        target = request.get("target", "all")
        barangay = request.get("barangay")
        user_id = request.get("user_id")

        if not title or not body:
            raise HTTPException(status_code=400, detail="Title and body are required")

        notif_id = str(uuid.uuid4())

        if target == "user" and user_id:
            # Direct per-user notification
            db.execute(
                text("""INSERT INTO notifications
                       (id, user_id, title, body, type, is_read, created_at)
                       VALUES (:id, :user_id, :title, :body, :type, FALSE, CURRENT_TIMESTAMP)"""),
                {
                    "id": notif_id,
                    "user_id": user_id,
                    "title": title,
                    "body": body,
                    "type": notification_type,
                },
            )
            sent_description = "1 user"

        elif target == "barangay" and barangay:
            # Broadcast to a barangay — one row, resolved at read time
            db.execute(
                text("""INSERT INTO notifications
                       (id, user_id, title, body, type, is_read,
                        target_scope, target_barangay, created_at)
                       VALUES (:id, NULL, :title, :body, :type, FALSE,
                               'barangay', :barangay, CURRENT_TIMESTAMP)"""),
                {
                    "id": notif_id,
                    "title": title,
                    "body": body,
                    "type": notification_type,
                    "barangay": barangay,
                },
            )
            sent_description = f"barangay {barangay}"

        else:
            # Broadcast to all users — one row, resolved at read time
            db.execute(
                text("""INSERT INTO notifications
                       (id, user_id, title, body, type, is_read,
                        target_scope, created_at)
                       VALUES (:id, NULL, :title, :body, :type, FALSE,
                               'all', CURRENT_TIMESTAMP)"""),
                {
                    "id": notif_id,
                    "title": title,
                    "body": body,
                    "type": notification_type,
                },
            )
            sent_description = "all users"

        db.commit()
        return {
            "success": True,
            "message": f"Notification sent to {sent_description}",
            "notification_id": notif_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/notifications/{user_id}")
async def get_notifications(
    user_id: str, limit: int = 100, offset: int = 0, current_user: dict = Depends(get_current_user_record)
):
    """Get user notifications (own, or any as admin).

    Returns both per-user notifications (user_id matches) and broadcast
    notifications whose target_scope/target_barangay/target_role match this
    user. Broadcasts that the user has dismissed are excluded. Read state
    for broadcasts comes from the notification_reads table.
    """
    _require_self_or_admin(current_user, user_id)
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=300)

        # Look up the user's barangay and role so we can match broadcasts.
        user_row = db.execute(
            text("SELECT barangay, role FROM users WHERE id = :uid"),
            {"uid": user_id},
        ).fetchone()
        user_barangay = user_row[0] if user_row else None
        user_role = user_row[1] if user_row else None

        _ensure_notification_reads_table(db)

        # UNION of:
        # 1) Per-user notifications (legacy + new direct ones)
        # 2) Broadcast 'all' notifications
        # 3) Broadcast 'barangay' notifications matching user's barangay
        # 4) Broadcast 'role' notifications matching user's role
        # Broadcasts that the user has dismissed are excluded via LEFT JOIN.
        if _is_sqlite(db):
            query = """
                SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                       COALESCE(nr.is_read, n.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                FROM notifications n
                LEFT JOIN notification_reads nr
                    ON nr.notification_id = n.id AND nr.user_id = :uid
                WHERE n.user_id = :uid
                  AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                UNION ALL

                SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                       COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                FROM notifications n
                LEFT JOIN notification_reads nr
                    ON nr.notification_id = n.id AND nr.user_id = :uid
                WHERE n.user_id IS NULL
                  AND n.target_scope = 'all'
                  AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                UNION ALL

                SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                       COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                FROM notifications n
                LEFT JOIN notification_reads nr
                    ON nr.notification_id = n.id AND nr.user_id = :uid
                WHERE n.user_id IS NULL
                  AND n.target_scope = 'barangay'
                  AND n.target_barangay = :barangay
                  AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                UNION ALL

                SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                       COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                FROM notifications n
                LEFT JOIN notification_reads nr
                    ON nr.notification_id = n.id AND nr.user_id = :uid
                WHERE n.user_id IS NULL
                  AND n.target_scope = 'role'
                  AND n.target_role = :role
                  AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """
        else:
            # MySQL: same logic, wrap in a subquery for ORDER BY + LIMIT
            query = """
                SELECT * FROM (
                    SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                           COALESCE(nr.is_read, n.is_read, 0) AS is_read,
                           -- Seconds, not the raw column: MariaDB TIMESTAMP
                           -- comes back as session-local wall clock, and
                           -- tagging that as UTC put every notification 8h in
                           -- the future on clients. CAST keeps it an integer
                           -- (UNIX_TIMESTAMP yields DECIMAL) so the existing
                           -- epoch branch of format_datetime_utc handles it.
                           CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                               AS created_at, n.target_scope
                    FROM notifications n
                    LEFT JOIN notification_reads nr
                        ON nr.notification_id = n.id AND nr.user_id = :uid
                    WHERE n.user_id = :uid
                      AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                    UNION ALL

                    SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                           COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                    FROM notifications n
                    LEFT JOIN notification_reads nr
                        ON nr.notification_id = n.id AND nr.user_id = :uid
                    WHERE n.user_id IS NULL
                      AND n.target_scope = 'all'
                      AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                    UNION ALL

                    SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                           COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                    FROM notifications n
                    LEFT JOIN notification_reads nr
                        ON nr.notification_id = n.id AND nr.user_id = :uid
                    WHERE n.user_id IS NULL
                      AND n.target_scope = 'barangay'
                      AND n.target_barangay = :barangay
                      AND (nr.dismissed IS NULL OR nr.dismissed = 0)

                    UNION ALL

                    SELECT n.id, :uid AS user_id, n.title, n.body, n.type,
                           COALESCE(nr.is_read, 0) AS is_read,
                            CAST(UNIX_TIMESTAMP(n.created_at) AS SIGNED)
                                AS created_at, n.target_scope
                    FROM notifications n
                    LEFT JOIN notification_reads nr
                        ON nr.notification_id = n.id AND nr.user_id = :uid
                    WHERE n.user_id IS NULL
                      AND n.target_scope = 'role'
                      AND n.target_role = :role
                      AND (nr.dismissed IS NULL OR nr.dismissed = 0)
                ) combined
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """

        result = db.execute(
            text(query),
            {
                "uid": user_id,
                "barangay": user_barangay or "",
                "role": user_role or "",
                "limit": limit,
                "offset": offset,
            },
        ).fetchall()

        notifications = []
        for row in result:
            notifications.append(
                {
                    "id": row[0],
                    "user_id": row[1],
                    "title": row[2],
                    "body": row[3],
                    "type": row[4],
                    "is_read": bool(row[5]) if row[5] is not None else False,
                    "created_at": format_datetime_utc(row[6]),
                }
            )
        return {"success": True, "data": notifications}
    finally:
        db.close()


@app.put("/api/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: str, current_user: dict = Depends(get_current_user_record)):
    """Mark notification as read. Works for both per-user and broadcast notifications."""
    db = SessionLocal()
    try:
        user_id = current_user.get("id")
        is_admin = current_user.get("role") == "admin"

        # Check if this is a broadcast notification (user_id IS NULL)
        notif = db.execute(
            text("SELECT user_id, target_scope FROM notifications WHERE id = :id"),
            {"id": notification_id},
        ).fetchone()

        if not notif:
            raise HTTPException(status_code=404, detail="Notification not found")

        if notif[0] is None and notif[1] is not None:
            # Broadcast notification — record read state in notification_reads
            _ensure_notification_reads_table(db)
            _upsert_notification_read(db, notification_id, user_id, is_read=True)
        else:
            # Per-user notification — update directly
            db.execute(
                text("UPDATE notifications SET is_read = TRUE WHERE id = :id AND (user_id = :uid OR :is_admin)"),
                {"id": notification_id, "uid": user_id, "is_admin": is_admin},
            )
        db.commit()
        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/notifications/read-all")
async def mark_all_notifications_read(request: dict, current_user: dict = Depends(get_current_user_record)):
    """Mark all notifications as read for a user (per-user + broadcasts)."""
    db = SessionLocal()
    try:
        user_id = request.get("user_id") or current_user.get("id")
        _require_self_or_admin(current_user, user_id)
        if user_id:
            # Mark per-user notifications
            db.execute(
                text(
                    "UPDATE notifications SET is_read = TRUE WHERE user_id = :user_id"
                ),
                {"user_id": user_id},
            )

            # Mark all unread broadcast notifications for this user
            _ensure_notification_reads_table(db)
            user_row = db.execute(
                text("SELECT barangay, role FROM users WHERE id = :uid"),
                {"uid": user_id},
            ).fetchone()
            user_barangay = user_row[0] if user_row else None
            user_role = user_row[1] if user_row else None

            # Find broadcast notifications this user can see but hasn't read
            broadcast_ids = db.execute(
                text("""
                    SELECT n.id FROM notifications n
                    LEFT JOIN notification_reads nr
                        ON nr.notification_id = n.id AND nr.user_id = :uid
                    WHERE n.user_id IS NULL
                      AND n.target_scope IS NOT NULL
                      AND (nr.is_read IS NULL OR nr.is_read = FALSE)
                      AND (
                          n.target_scope = 'all'
                          OR (n.target_scope = 'barangay' AND n.target_barangay = :barangay)
                          OR (n.target_scope = 'role' AND n.target_role = :role)
                      )
                """),
                {"uid": user_id, "barangay": user_barangay or "", "role": user_role or ""},
            ).fetchall()

            for row in broadcast_ids:
                _upsert_notification_read(db, row[0], user_id, is_read=True)

            db.commit()
        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# Registered before /{notification_id} so "old" is not captured as an id.
@app.delete("/api/notifications/old")
async def delete_old_notifications(days: int = 30, current_user: dict = Depends(require_admin)):
    """Delete notifications older than specified days. Admin only."""
    db = SessionLocal()
    try:
        if db.get_bind().dialect.name == "sqlite":
            cleanup_sql = (
                "DELETE FROM notifications WHERE created_at < "
                "datetime('now', '-' || :days || ' days')"
            )
            reads_cleanup_sql = (
                "DELETE FROM notification_reads WHERE notification_id NOT IN "
                "(SELECT id FROM notifications)"
            )
        else:
            cleanup_sql = (
                "DELETE FROM notifications WHERE created_at < "
                "DATE_SUB(CURRENT_TIMESTAMP, INTERVAL :days DAY)"
            )
            reads_cleanup_sql = (
                "DELETE FROM notification_reads WHERE notification_id NOT IN "
                "(SELECT id FROM notifications)"
            )
        result = db.execute(text(cleanup_sql), {"days": days})
        deleted = result.rowcount
        # Clean up orphaned notification_reads rows
        _ensure_notification_reads_table(db)
        db.execute(text(reads_cleanup_sql))
        db.commit()
        return {"success": True, "deleted_count": deleted}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/notifications/{notification_id}")
async def delete_notification(notification_id: str, current_user: dict = Depends(get_current_user_record)):
    """Delete a notification (own, or any as admin).

    For broadcast notifications, this dismisses it for the current user
    rather than deleting the shared row (other users still see it).
    Admins can delete the broadcast row itself to remove it for everyone.
    """
    db = SessionLocal()
    try:
        user_id = current_user.get("id")
        is_admin = current_user.get("role") == "admin"

        notif = db.execute(
            text("SELECT user_id, target_scope FROM notifications WHERE id = :id"),
            {"id": notification_id},
        ).fetchone()

        if not notif:
            raise HTTPException(status_code=404, detail="Notification not found")

        if notif[0] is None and notif[1] is not None:
            if is_admin:
                # Admin deletes the broadcast row entirely
                db.execute(
                    text("DELETE FROM notifications WHERE id = :id"),
                    {"id": notification_id},
                )
                db.execute(
                    text("DELETE FROM notification_reads WHERE notification_id = :id"),
                    {"id": notification_id},
                )
            else:
                # Non-admin: dismiss the broadcast for this user only
                _ensure_notification_reads_table(db)
                _upsert_notification_read(db, notification_id, user_id, dismissed=True)
        else:
            # Per-user notification — delete directly
            db.execute(
                text("DELETE FROM notifications WHERE id = :id AND (user_id = :uid OR :is_admin)"),
                {"id": notification_id, "uid": user_id, "is_admin": is_admin},
            )
        db.commit()
        return {"success": True, "message": "Notification deleted"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/notifications/user/{user_id}")
async def delete_all_user_notifications(user_id: str, current_user: dict = Depends(get_current_user_record)):
    """Delete all notifications for a user (per-user + dismiss broadcasts)."""
    _require_self_or_admin(current_user, user_id)
    db = SessionLocal()
    try:
        # Delete per-user notifications
        result = db.execute(
            text("DELETE FROM notifications WHERE user_id = :user_id"),
            {"user_id": user_id},
        )
        deleted = result.rowcount

        # Dismiss all visible broadcasts for this user
        _ensure_notification_reads_table(db)
        user_row = db.execute(
            text("SELECT barangay, role FROM users WHERE id = :uid"),
            {"uid": user_id},
        ).fetchone()
        user_barangay = user_row[0] if user_row else None
        user_role = user_row[1] if user_row else None

        broadcast_ids = db.execute(
            text("""
                SELECT n.id FROM notifications n
                LEFT JOIN notification_reads nr
                    ON nr.notification_id = n.id AND nr.user_id = :uid
                WHERE n.user_id IS NULL
                  AND n.target_scope IS NOT NULL
                  AND (nr.dismissed IS NULL OR nr.dismissed = 0)
                  AND (
                      n.target_scope = 'all'
                      OR (n.target_scope = 'barangay' AND n.target_barangay = :barangay)
                      OR (n.target_scope = 'role' AND n.target_role = :role)
                  )
            """),
            {"uid": user_id, "barangay": user_barangay or "", "role": user_role or ""},
        ).fetchall()

        for row in broadcast_ids:
            _upsert_notification_read(db, row[0], user_id, dismissed=True)

        db.commit()
        return {"success": True, "deleted_count": deleted, "dismissed_broadcasts": len(broadcast_ids)}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/notifications/{notification_id}")
async def update_notification(notification_id: str, request: dict, current_user: dict = Depends(get_current_user_record)):
    """Update notification (read state). Works for both per-user and broadcasts."""
    db = SessionLocal()
    try:
        is_read = request.get("is_read")
        if is_read is not None:
            user_id = current_user.get("id")
            is_admin = current_user.get("role") == "admin"

            notif = db.execute(
                text("SELECT user_id, target_scope FROM notifications WHERE id = :id"),
                {"id": notification_id},
            ).fetchone()

            if notif and notif[0] is None and notif[1] is not None:
                # Broadcast — update via notification_reads
                _ensure_notification_reads_table(db)
                _upsert_notification_read(db, notification_id, user_id, is_read=bool(is_read))
            else:
                db.execute(
                    text("UPDATE notifications SET is_read = :is_read WHERE id = :id AND (user_id = :uid OR :is_admin)"),
                    {"is_read": is_read, "id": notification_id, "uid": user_id, "is_admin": is_admin},
                )
        db.commit()
        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/app-update")
async def get_app_update():
    """Public endpoint — returns latest app update info."""
    db = SessionLocal()
    try:
        row = db.execute(
            text("SELECT latest_version, update_url, force_update FROM app_update WHERE id = 'app'")
        ).fetchone()
        if row and row[1]:
            return {
                "latest_version": row[0] or "",
                "update_url": row[1] or "",
                "force_update": bool(row[2]),
            }
        return {"latest_version": "", "update_url": "", "force_update": False}
    finally:
        db.close()


@app.post("/api/app-update")
async def set_app_update(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Admin-only — store or update the app update link + version."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    update_url = request.get("update_url", "")
    latest_version = request.get("latest_version", "")
    force_update = request.get("force_update", False)

    if not update_url or not latest_version:
        raise HTTPException(
            status_code=400,
            detail="update_url and latest_version are required",
        )

    db = SessionLocal()
    try:
        _store_app_update(db, latest_version, update_url, force_update)
        db.commit()
        return {"success": True, "latest_version": latest_version, "update_url": update_url}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/auth/create-user")
async def create_user(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Create a new user (admin only)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        email = request.get("email", "").strip().lower()
        password = request.get("password", "")
        full_name = request.get("full_name", "")
        role = request.get("role", "resident")
        barangay = request.get("barangay", None)
        phone = request.get("phone", None)

        if not email or not password or not full_name:
            raise HTTPException(
                status_code=400, detail="Email, password, and full name are required"
            )

        existing = db.execute(
            text("SELECT id FROM users WHERE email = :email"),
            {"email": email},
        ).fetchone()

        if existing:
            raise HTTPException(status_code=400, detail="Email already registered")

        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = str(uuid.uuid4())

        db.execute(
            text("""
                INSERT INTO users (id, email, full_name, password, role, barangay, phone, is_active, created_at)
                VALUES (:id, :email, :full_name, :password, :role, :barangay, :phone, TRUE, CURRENT_TIMESTAMP)
            """),
            {
                "id": user_id,
                "email": email,
                "full_name": full_name,
                "password": hashed,
                "role": role,
                "barangay": barangay,
                "phone": phone,
            },
        )
        db.commit()

        return {
            "success": True,
            "data": {
                "user": {
                    "id": user_id,
                    "email": email,
                    "full_name": full_name,
                    "role": role,
                    "barangay": barangay,
                    "is_active": True,
                    "assigned_truck_plate": None,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/auth/profile/settings")
async def update_profile_settings(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Update notification settings for current user."""
    db = SessionLocal()
    try:
        # For now, just return success - settings can be expanded later
        return {"success": True, "message": "Settings updated"}
    finally:
        db.close()


@app.put("/api/drivers/{driver_id}/assign-truck")
async def assign_truck_to_driver(driver_id: str, request: dict, current_user: dict = Depends(require_admin)):
    """Assign a truck plate to a driver."""
    db = SessionLocal()
    try:
        truck_plate = request.get("truck_plate")

        existing = db.execute(
            text("SELECT id, full_name FROM users WHERE id = :id AND role = 'driver'"),
            {"id": driver_id},
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Driver not found")

        db.execute(
            text("UPDATE users SET assigned_truck_plate = :truck_plate WHERE id = :id"),
            {
                "truck_plate": truck_plate if truck_plate != "" else None,
                "id": driver_id,
            },
        )
        db.commit()

        return {
            "success": True,
            "message": f"Truck {truck_plate or 'removed'} assigned to driver {existing[1]}",
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/users/{user_id}")
async def update_user(
    user_id: str, request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Update user details including assigned_truck_plate."""
    db = SessionLocal()
    try:
        assigned_truck_plate = request.get("assigned_truck_plate")
        barangay = request.get("barangay")
        full_name = request.get("full_name")

        update_fields = []
        params = {"id": user_id}

        if assigned_truck_plate is not None:
            update_fields.append("assigned_truck_plate = :assigned_truck_plate")
            params["assigned_truck_plate"] = (
                assigned_truck_plate if assigned_truck_plate != "" else None
            )

        if barangay is not None:
            update_fields.append("barangay = :barangay")
            params["barangay"] = barangay

        if full_name is not None:
            update_fields.append("full_name = :full_name")
            params["full_name"] = full_name

        if not update_fields:
            return {"success": True, "message": "No fields to update"}

        db.execute(
            text(f"UPDATE users SET {', '.join(update_fields)} WHERE id = :id"),
            params,
        )
        db.commit()
        return {"success": True, "message": "User updated successfully"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/users/{user_id}/role")
async def update_user_role(
    user_id: str, request: dict, current_user: dict = Depends(get_current_user_record)
):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    new_role = request.get("role")
    if not new_role or new_role not in ["admin", "driver", "resident"]:
        raise HTTPException(status_code=400, detail="Valid role is required")

    if user_id == current_user.get("id"):
        raise HTTPException(status_code=400, detail="Cannot change your own role")

    db = SessionLocal()
    try:
        existing = db.execute(
            text("SELECT role FROM users WHERE id = :id"), {"id": user_id}
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="User not found")

        if existing[0] == "admin" and new_role != "admin":
            raise HTTPException(status_code=400, detail="Cannot demote another admin")

        db.execute(
            text("UPDATE users SET role = :role WHERE id = :id"),
            {"role": new_role, "id": user_id},
        )
        db.commit()
        return {"success": True, "message": f"User role updated to {new_role}"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/users/{user_id}/toggle-status")
async def toggle_user_status(
    user_id: str, current_user: dict = Depends(get_current_user_record)
):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        user = db.execute(
            text("SELECT is_active FROM users WHERE id = :id"), {"id": user_id}
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        current_status = user[0] if user[0] is not None else True
        new_status = not current_status

        db.execute(
            text("UPDATE users SET is_active = :status WHERE id = :id"),
            {"status": new_status, "id": user_id},
        )
        db.commit()
        return {
            "success": True,
            "message": "User status updated",
            "data": {"is_active": new_status},
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/users/{user_id}")
async def delete_user(
    user_id: str, current_user: dict = Depends(get_current_user_record)
):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        user = db.execute(
            text("SELECT id FROM users WHERE id = :id"), {"id": user_id}
        ).fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        db.execute(text("DELETE FROM users WHERE id = :id"), {"id": user_id})
        db.commit()
        return {"success": True, "message": "User deleted successfully"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= REPORTS =============


@app.get("/api/reports")
async def get_reports(
    barangay: str = None,
    status: str = None,
    limit: int = 100,
    offset: int = 0,
    response: Response = None,
    current_user: dict = Depends(get_current_user_record),
):
    """Get all reports. Optional status filter (pending, in_progress, resolved, dismissed)."""
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=200)
        cache_key = f"{current_user.get('role')}|{current_user.get('id')}|{barangay}|{status}|{limit}|{offset}"
        cached_reports = _cache_get(_reports_cache, cache_key, REPORTS_CACHE_SECONDS)
        if cached_reports is not None:
            if response:
                response.headers["X-Cache"] = "HIT"
            return {"success": True, "data": cached_reports}

        created_at_column = report_created_at_column(db.get_bind().dialect.name)
        query = f"SELECT id, user_id, barangay, description, category, latitude, longitude, address, status, image_url, {created_at_column} FROM reports WHERE 1=1"
        params = {}

        if barangay:
            query += " AND barangay = :barangay"
            params["barangay"] = barangay
        elif current_user.get("role") == "resident":
            query += " AND user_id = :user_id"
            params["user_id"] = current_user.get("id")

        if status and status != "all":
            query += " AND status = :status"
            params["status"] = status

        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        result = db.execute(text(query), params).fetchall()
        reports = []
        for row in result:
            reports.append(
                {
                    "id": row[0],
                    "user_id": row[1],
                    "barangay": row[2],
                    "description": row[3],
                    "category": row[4],
                    "latitude": float(row[5]) if row[5] else None,
                    "longitude": float(row[6]) if row[6] else None,
                    "address": row[7] if len(row) > 7 else None,
                    "status": row[8] if len(row) > 8 else "pending",
                    "image_url": row[9] if len(row) > 9 else None,
                    "created_at": format_datetime_utc(row[10]) if len(row) > 10 else None,
                }
            )
        _cache_set(_reports_cache, cache_key, reports)
        if response:
            response.headers["X-Cache"] = "MISS"
        return {"success": True, "data": reports}
    except Exception as e:
        print(f"ERROR in get_reports: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/reports/{report_id}")
async def get_report(report_id: str, current_user: dict = Depends(get_current_user_record)):
    """Get a specific report."""
    db = SessionLocal()
    try:
        created_at_column = report_created_at_column(db.get_bind().dialect.name)
        result = db.execute(
            text(f"SELECT reports.*, {created_at_column} FROM reports WHERE id = :id"),
            {"id": report_id},
        ).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Report not found")
        m = result._mapping
        return {
            "success": True,
            "data": {
                "id": m.get("id"),
                "user_id": m.get("user_id"),
                "barangay": m.get("barangay"),
                "description": m.get("description"),
                "category": m.get("category"),
                "latitude": float(m.get("latitude")) if m.get("latitude") else None,
                "longitude": float(m.get("longitude")) if m.get("longitude") else None,
                "address": m.get("address"),
                "status": m.get("status"),
                "image_url": m.get("image_url"),
                "created_at": format_datetime_utc(m.get("created_at_utc")),
            },
        }
    finally:
        db.close()


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), current_user: dict = Depends(get_current_user_record)):
    """Upload an image file and return its URL."""
    import shutil
    filename = f"{uuid.uuid4()}{os.path.splitext(file.filename or '.jpg')[1]}"
    filepath = os.path.join(UPLOAD_DIR, filename)
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return {"success": True, "data": {"url": f"/uploads/{filename}"}}


# ---------------------------------------------------------------------------
# Waste classification (server-side inference)
# ---------------------------------------------------------------------------
# The phone no longer classifies. Shipping the model inside the APK meant a
# wrong class could only be fixed by rebuilding and reinstalling on every
# device; hosting it here means replacing one file.
#
# ONNX rather than TFLite because tflite-runtime has no Windows wheel and the
# server runs as a PyInstaller EXE on the client's Windows machine. The export
# was verified numerically identical to the on-device TFLite model.
_waste_classifier = None
_waste_classifier_lock = threading.Lock()


def waste_model_dir() -> Path:
    """Locate the bundled model files.

    Two layouts matter:
      * running from source  -> <repo>/assets/model
      * running as the packaged EXE -> sys._MEIPASS/assets/model

    PyInstaller one-file extracts the bundle to a temp dir and sets
    sys._MEIPASS, but __file__ still points at the frozen module, so
    Path(__file__).parent alone would resolve to a directory that does not
    contain the model and the classifier would silently report "unavailable".
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass) / "assets" / "model"
    return Path(__file__).resolve().parent / "assets" / "model"


def get_waste_classifier():
    """Lazily construct the classifier so a missing model cannot block boot."""
    global _waste_classifier
    if _waste_classifier is None:
        with _waste_classifier_lock:
            if _waste_classifier is None:
                from waste_classifier import WasteClassifier

                _waste_classifier = WasteClassifier(waste_model_dir())
    return _waste_classifier


@app.get("/api/waste/model-info")
async def waste_model_info():
    """Report whether the classifier is ready, and what it can predict.

    Public and side-effect free so the panel and the app can check health
    without uploading a photo.
    """
    clf = get_waste_classifier()
    return {
        "success": True,
        "data": {
            "available": clf.available,
            "error": clf.load_error,
            "labels": clf.labels,
            "image_size": 224,
        },
    }


@app.post("/api/waste/classify")
async def classify_waste(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user_record),
):
    """Classify an uploaded photo and return the category plus bin guidance.

    Returns the full probability vector as well as the decision. That is the
    point of moving inference to the server: when a resident reports "my bottle
    came out wrong", the vector shows whether the model was confidently wrong
    or merely below the gate.
    """
    clf = get_waste_classifier()
    if not clf.available:
        raise HTTPException(
            status_code=503,
            detail=clf.load_error or "waste model unavailable",
        )

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty image upload")
    if len(raw) > 12 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Image larger than 12 MB")

    try:
        probabilities = await asyncio.to_thread(clf.predict_proba, raw)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Waste classification failed")
        raise HTTPException(
            status_code=500, detail=f"classification failed: {exc}"
        ) from exc

    decision = clf.decide(probabilities)

    # Log every scan with its vector. Without this there is no way to build a
    # dataset of what residents actually photograph, which is the only thing
    # that will fix the weak classes.
    logging.info(
        "waste scan user=%s category=%s conf=%.3f top=%s probs=%s",
        current_user.get("id"),
        decision["category"],
        decision["confidence"],
        decision.get("top_label"),
        decision.get("probabilities"),
    )

    return {"success": True, "data": decision}



@app.put("/api/app-update/upload")
async def upload_app_update(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user_record),
    version: str = None,
    force: bool = False,
    filename: str = None,
    update_db: bool = None,
):
    """Admin uploads APK directly to server, sets as update.

    Backward compatible: omit `filename` to get the legacy single-APK behavior
    (file saved as etrak-v{version}.apk, other etrak-v*.apk files removed,
    app_update row updated).

    Pass `filename` to store under a custom name without touching other APKs.
    Pass `update_db=false` to just store the file without touching the app_update
    row (used for ABIs other than the primary, plus non-APK artifacts).
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    import shutil

    if filename:
        safe_name = os.path.basename(filename)
        if not safe_name or safe_name.startswith("."):
            raise HTTPException(status_code=400, detail="invalid filename")
        do_update_db = bool(update_db) if update_db is not None else False
        do_cleanup = False
    else:
        if not version:
            raise HTTPException(status_code=400, detail="version query param required")
        safe_name = f"etrak-v{version}.apk"
        do_update_db = bool(update_db) if update_db is not None else True
        do_cleanup = True

    filepath = os.path.join(UPLOAD_DIR, safe_name)
    with open(filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Only clean up old default-named APKs when uploading the primary APK
    if do_cleanup:
        for old in glob.glob(os.path.join(UPLOAD_DIR, "etrak-v*.apk")):
            if old != filepath:
                os.remove(old)

    if not do_update_db:
        return {
            "success": True,
            "url": f"/uploads/{safe_name}",
        }

    if not version:
        raise HTTPException(status_code=400, detail="version query param required")

    update_url = f"/uploads/{safe_name}"
    db = SessionLocal()
    try:
        _store_app_update(db, version, update_url, force)
        db.commit()
        return {
            "success": True,
            "latest_version": version,
            "update_url": update_url,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/reports")
async def create_report(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Create a new report."""
    db = SessionLocal()
    try:
        description = request.get("description", "")
        category = request.get("category", "general")
        barangay = request.get("barangay") or current_user.get("barangay")
        latitude = request.get("latitude")
        longitude = request.get("longitude")
        address = request.get("address")
        image_url = request.get("image_url")

        report_id = str(uuid.uuid4())

        db.execute(
            text("""
                INSERT INTO reports (id, user_id, barangay, description, category, latitude, longitude, address, image_url, created_at)
                VALUES (:id, :user_id, :barangay, :description, :category, :latitude, :longitude, :address, :image_url, CURRENT_TIMESTAMP)
            """),
            {
                "id": report_id,
                "user_id": current_user.get("id"),
                "barangay": barangay,
                "description": description,
                "category": category,
                "latitude": latitude,
                "longitude": longitude,
                "address": address,
                "image_url": image_url,
            },
        )

        # Update user reports count and add +2 points
        db.execute(
            text(
                "UPDATE users SET reports_submitted = reports_submitted + 1, total_points = total_points + 2, lifetime_points = lifetime_points + 2 WHERE id = :id"
            ),
            {"id": current_user.get("id")},
        )

        db.commit()
        _reports_cache.clear()
        return {"success": True, "data": {"id": report_id}}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/reports/{report_id}")
async def update_report(report_id: str, request: dict, current_user: dict = Depends(get_current_user_record)):
    """Update a report."""
    db = SessionLocal()
    try:
        status = request.get("status")
        update_fields = []
        params = {"id": report_id}

        if status:
            update_fields.append("status = :status")
            params["status"] = status

        if update_fields:
            db.execute(
                text(f"UPDATE reports SET {', '.join(update_fields)} WHERE id = :id"),
                params,
            )
            db.commit()
            _reports_cache.clear()

        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/reports/{report_id}/status")
async def update_report_status(report_id: str, request: dict, current_user: dict = Depends(require_admin)):
    """Update report status (admin)."""
    db = SessionLocal()
    try:
        status = request.get("status")
        admin_note = request.get("adminNote")

        # Get old status and user_id first
        old_report = db.execute(
            text("SELECT status, user_id FROM reports WHERE id = :id"),
            {"id": report_id}
        ).fetchone()

        update_fields = []
        params = {"id": report_id}

        if status:
            update_fields.append("status = :status")
            params["status"] = status

        if admin_note:
            update_fields.append("admin_note = :admin_note")
            params["admin_note"] = admin_note

        if update_fields:
            db.execute(
                text(f"UPDATE reports SET {', '.join(update_fields)} WHERE id = :id"),
                params,
            )

            # Award bonus points if status changed to resolved and wasn't resolved before
            if status == "resolved" and old_report and old_report[0] != "resolved" and old_report[1]:
                db.execute(
                    text(
                        "UPDATE users SET total_points = total_points + 3, lifetime_points = lifetime_points + 3 WHERE id = :user_id AND role = 'resident'"
                    ),
                    {"user_id": old_report[1]}
                )

            db.commit()
        _reports_cache.clear()

        return {"success": True}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/reports/{report_id}")
async def delete_report(
    report_id: str,
    current_user: dict = Depends(get_current_user_record),
):
    """Delete a single report. Owner (resident) or admin only."""
    if current_user.get("role") not in ("admin", "resident"):
        raise HTTPException(status_code=403, detail="Not authorized")
    db = SessionLocal()
    try:
        if current_user.get("role") == "resident":
            owner = db.execute(
                text("SELECT user_id FROM reports WHERE id = :id"),
                {"id": report_id},
            ).first()
            if not owner or owner[0] != current_user.get("id"):
                raise HTTPException(status_code=403, detail="Not your report")
        db.execute(text("DELETE FROM reports WHERE id = :id"), {"id": report_id})
        db.commit()
        _reports_cache.clear()
        return {"success": True, "deleted": report_id}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/admin/reports")
async def reset_all_reports(current_user: dict = Depends(get_current_user_record)):
    """Admin only: wipe all reports. For clean thesis evidence runs."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    db = SessionLocal()
    try:
        result = db.execute(text("DELETE FROM reports"))
        db.commit()
        _reports_cache.clear()
        return {"success": True, "deleted": result.rowcount}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= ANNOUNCEMENTS =============


class AnnouncementCreate(BaseModel):
    title: str
    content: str
    is_urgent: bool = False
    target_role: str = "all"
    target_barangay: Optional[str] = None


class AnnouncementUpdate(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None
    is_active: Optional[bool] = None


@app.get("/api/announcements")
async def get_announcements(user_role: str = "all", user_barangay: str = None, response: Response = None):
    """Get all announcements or filtered by user role/barangay"""
    db = SessionLocal()
    try:
        query = """
            SELECT id, title, content, is_active, is_urgent,
                   target_role, target_barangay, created_at, created_by
            FROM announcements
            WHERE is_active = TRUE
        """
        params = {}

        # Filter by role if specified (not 'all')
        if user_role != "all" and user_role in ["admin", "driver", "resident"]:
            query += " AND (target_role = :target_role OR target_role = 'all')"
            params["target_role"] = user_role

        # Filter by barangay if specified
        if user_barangay:
            query += " AND (target_barangay = :barangay OR target_barangay IS NULL)"
            params["barangay"] = user_barangay

        query += " ORDER BY is_urgent DESC, created_at DESC"

        result = db.execute(text(query), params).fetchall()

        announcements = []
        for row in result:
            mapping = row._mapping if hasattr(row, "_mapping") else None
            if mapping:
                announcements.append(dict(mapping))
        if response:
            # Announcements are edited from the admin screen and are expected to
            # appear promptly for residents, so allow only a short cache window.
            # A longer max-age here makes newly created announcements invisible.
            response.headers["Cache-Control"] = "private, max-age=15"
        return {"success": True, "data": announcements}
    finally:
        db.close()


@app.post("/api/announcements")
async def create_announcement(
    announcement: AnnouncementCreate,
    current_user: dict = Depends(get_current_user_record),
):
    """Create a new announcement (admin only)"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        announcement_id = str(uuid.uuid4())
        created_by = current_user.get("id")

        db.execute(
            text("""
                INSERT INTO announcements (id, title, content, is_urgent,
                                         target_role, target_barangay, created_by)
                VALUES (:id, :title, :content, :is_urgent,
                        :target_role, :target_barangay, :created_by)
            """),
            {
                "id": announcement_id,
                "title": announcement.title,
                "content": announcement.content,
                "is_urgent": announcement.is_urgent,
                "target_role": announcement.target_role,
                "target_barangay": announcement.target_barangay,
                "created_by": created_by,
            },
        )
        db.commit()

        # Also raise a notification so targeted users actually learn about the
        # announcement. A single broadcast row is inserted (user_id NULL) and the
        # read endpoint resolves it per user via target_scope, so there is no
        # fan-out loop. Failure here must not lose the announcement itself.
        notification_sent = False
        try:
            if announcement.target_barangay:
                scope, notif_role = "barangay", None
            elif announcement.target_role and announcement.target_role != "all":
                scope, notif_role = "role", announcement.target_role
            else:
                scope, notif_role = "all", None

            title_prefix = "Urgent: " if announcement.is_urgent else ""
            db.execute(
                text("""
                    INSERT INTO notifications
                    (id, user_id, title, body, type, is_read,
                     target_scope, target_barangay, target_role, created_at)
                    VALUES (:id, NULL, :title, :body, :type, FALSE,
                            :scope, :barangay, :role, CURRENT_TIMESTAMP)
                """),
                {
                    "id": str(uuid.uuid4()),
                    "title": f"{title_prefix}{announcement.title}",
                    "body": announcement.content,
                    "type": "announcement",
                    # Keep within the notifications.type VARCHAR(20) limit.
                    "scope": scope,
                    "barangay": announcement.target_barangay,
                    "role": notif_role,
                },
            )
            db.commit()
            notification_sent = True
        except Exception as notify_error:
            db.rollback()
            logging.getLogger("etrak_server").warning(
                "Announcement %s created but notification failed: %s",
                announcement_id,
                notify_error,
            )

        return {
            "success": True,
            "message": "Announcement created successfully",
            "data": {"id": announcement_id, "notification_sent": notification_sent},
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/announcements/{announcement_id}")
async def update_announcement(
    announcement_id: str,
    announcement: AnnouncementUpdate,
    current_user: dict = Depends(get_current_user_record),
):
    """Update an announcement (admin only)"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        # Check if announcement exists
        existing = db.execute(
            text("SELECT id FROM announcements WHERE id = :id"), {"id": announcement_id}
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Announcement not found")

        # Build update query
        updates = []
        params = {"id": announcement_id}

        if announcement.title is not None:
            updates.append("title = :title")
            params["title"] = announcement.title
        if announcement.content is not None:
            updates.append("content = :content")
            params["content"] = announcement.content
        if announcement.is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = announcement.is_active

        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        params_str = ", ".join(updates)
        db.execute(
            text(f"UPDATE announcements SET {params_str} WHERE id = :id"),
            params,
        )
        db.commit()
        return {"success": True, "message": "Announcement updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/announcements/{announcement_id}")
async def delete_announcement(
    announcement_id: str, current_user: dict = Depends(get_current_user_record)
):
    """Delete an announcement (admin only)"""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    db = SessionLocal()
    try:
        existing = db.execute(
            text("SELECT id FROM announcements WHERE id = :id"), {"id": announcement_id}
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Announcement not found")

        db.execute(
            text("DELETE FROM announcements WHERE id = :id"),
            {"id": announcement_id},
        )
        db.commit()
        return {"success": True, "message": "Announcement deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= SCHEDULES =============


@app.get("/api/schedules")
async def get_all_schedules(
    barangay: str = None,
    driver_id: str = None,
    current_user: dict = Depends(get_current_user_record),
    response: Response = None,
):
    db = SessionLocal()
    try:
        query = """SELECT s.*, u.full_name as driver_name
                FROM schedules s
                LEFT JOIN users u ON s.driver_id = u.id
                WHERE s.is_active = TRUE"""
        params = {}
        scope, scope_params = schedule_scope(current_user)
        query += scope
        params.update(scope_params)

        # Filter by user's barangay if they are a resident
        user_barangay = current_user.get("barangay")
        user_role = current_user.get("role", "resident")

        if barangay:
            query += " AND s.barangay = :barangay"
            params["barangay"] = barangay
        elif user_role == "resident" and user_barangay:
            # Residents only see their barangay's schedules
            query += " AND s.barangay = :user_barangay"
            params["user_barangay"] = user_barangay

        # Filter by driver_id if provided (for viewing driver's assigned schedules)
        if driver_id:
            query += " AND s.driver_id = :driver_id"
            params["driver_id"] = driver_id

        query += " ORDER BY s.day_of_week, s.start_time"

        result = db.execute(text(query), params).fetchall()

        schedules = []
        for row in result:
            mapping = row._mapping
            schedules.append(
                {
                    "id": mapping.get("id"),
                    **route_payload(mapping),
                    "barangay": mapping.get("barangay"),
                    "day_of_week": mapping.get("day_of_week", 0),
                    "waste_type": mapping.get("collection_type", ""),
                    "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                    "is_active": bool(mapping.get("is_active", True)),
                    "driver_id": mapping.get("driver_id"),
                    "assigned_driver_id": mapping.get("driver_id"),
                    "assigned_driver_name": mapping.get("driver_name"),
                }
            )
        if response:
            response.headers["Cache-Control"] = "private, max-age=300"
        return {"success": True, "data": schedules}
    finally:
        db.close()


@app.get("/api/schedules/today")
async def get_today_schedules(current_user: dict = Depends(get_current_user_record)):
    db = SessionLocal()
    try:
        # Python weekday(): Mon=0..Sun=6, matches DB day_of_week seeding (0=Monday)
        today = get_ph_weekday()

        user_barangay = current_user.get("barangay")
        user_role = current_user.get("role", "resident")

        query = """SELECT s.*, u.full_name as driver_name 
                FROM schedules s 
                LEFT JOIN users u ON s.driver_id = u.id
                WHERE s.day_of_week = :today AND s.is_active = TRUE"""
        params = {"today": today}
        scope, scope_params = schedule_scope(current_user)
        query += scope
        params.update(scope_params)

        # Residents only see their barangay's schedules
        if user_role == "resident" and user_barangay:
            query += " AND s.barangay = :user_barangay"
            params["user_barangay"] = user_barangay

        query += " ORDER BY s.start_time"

        result = db.execute(text(query), params).fetchall()

        schedules = []
        for row in result:
            mapping = row._mapping
            schedules.append(
                {
                    "id": mapping.get("id"),
                    **route_payload(mapping),
                    "barangay": mapping.get("barangay"),
                    "day_of_week": mapping.get("day_of_week", 0),
                    "waste_type": mapping.get("collection_type", ""),
                    "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                    "is_active": bool(mapping.get("is_active", True)),
                    "driver_id": mapping.get("driver_id"),
                    "assigned_driver_id": mapping.get("driver_id"),
                    "assigned_driver_name": mapping.get("driver_name"),
                }
            )
        return {"success": True, "data": schedules}
    finally:
        db.close()


@app.get("/api/schedules/upcoming")
async def get_upcoming_schedules(
    dayOfWeek: int = None,
    limit: int = 3,
    current_user: dict = Depends(get_current_user_record),
):
    """Get upcoming schedules for the next N days starting from dayOfWeek."""
    db = SessionLocal()
    try:
        if dayOfWeek is None:
            dayOfWeek = get_ph_weekday()

        limit = min(limit, 7)

        scope, params = schedule_scope(current_user)
        params.update({"dayOfWeek": dayOfWeek, "limit": limit})
        result = db.execute(
            text("""SELECT s.*, u.full_name as driver_name
                    FROM schedules s LEFT JOIN users u ON s.driver_id = u.id
                    WHERE s.day_of_week >= :dayOfWeek AND s.is_active = TRUE"""
                 + scope + " ORDER BY s.day_of_week, s.start_time LIMIT :limit"),
            params,
        ).fetchall()

        schedules = []
        for row in result:
            mapping = row._mapping
            schedules.append(
                {
                    "id": mapping.get("id"),
                    **route_payload(mapping),
                    "barangay": mapping.get("barangay"),
                    "day_of_week": mapping.get("day_of_week", 0),
                    "waste_type": mapping.get("collection_type", ""),
                    "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                    "is_active": bool(mapping.get("is_active", True)),
                    "driver_id": mapping.get("driver_id"),
                    "assigned_driver_name": mapping.get("driver_name"),
                }
            )
        return {"success": True, "data": schedules}
    finally:
        db.close()


@app.post("/api/schedules")
async def create_schedule(
    schedule: ScheduleCreate, current_user: dict = Depends(require_admin)
):
    db = SessionLocal()
    try:
        try:
            validate_schedule(schedule.dict())
            validate_driver(db, schedule.driver_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error))
        schedule_id = str(uuid.uuid4())

        # Split time slot back into start and end time for db
        time_parts = schedule.time_slot.split("-")
        start_time = time_parts[0].strip() if len(time_parts) > 0 else ""
        end_time = time_parts[1].strip() if len(time_parts) > 1 else ""

        db.execute(
            text("""INSERT INTO schedules 
               (id, barangay, day_of_week, collection_type, start_time, 
                end_time, driver_id, route_name, collection_points, is_active, created_at)
               VALUES (:id, :barangay, :day_of_week, :collection_type,
                       :start_time, :end_time, :driver_id, :route_name, :collection_points, TRUE, CURRENT_TIMESTAMP)"""),
            {
                "id": schedule_id,
                "barangay": schedule.barangay,
                "day_of_week": schedule.day_of_week,
                "collection_type": schedule.waste_type,
                "start_time": start_time,
                "end_time": end_time,
                "driver_id": schedule.driver_id,
                "route_name": schedule.route_name,
                "collection_points": json.dumps(schedule.collection_points),
            },
        )
        db.commit()

        # Alert the assigned driver immediately when the shift is created.
        if schedule.driver_id:
            days_map = {
                0: "Monday",
                1: "Tuesday",
                2: "Wednesday",
                3: "Thursday",
                4: "Friday",
                5: "Saturday",
                6: "Sunday",
            }
            try:
                day_idx = int(schedule.day_of_week)
            except (TypeError, ValueError):
                day_idx = -1
            day_name = days_map.get(day_idx, "Scheduled day")
            try:
                db.execute(
                    text("""INSERT INTO notifications 
                       (id, user_id, title, body, type, is_read, created_at)
                       VALUES (:id, :user_id, :title, :body, :type, FALSE, CURRENT_TIMESTAMP)"""),
                    {
                        "id": str(uuid.uuid4()),
                        "user_id": schedule.driver_id,
                        "title": "New shift assigned",
                        "body": f"{schedule.barangay} • {day_name}, {schedule.time_slot} ({schedule.waste_type})",
                        "type": "shift",
                    },
                )
                db.commit()
            except Exception as notify_error:
                db.rollback()
                logging.warning(
                    f"Schedule created but driver notification failed: {notify_error}"
                )

        # Get driver name if assigned
        driver_name = None
        if schedule.driver_id:
            driver = db.execute(
                text("SELECT full_name FROM users WHERE id = :id"),
                {"id": schedule.driver_id},
            ).fetchone()
            driver_name = driver[0] if driver else None

        return {
            "success": True,
            "message": "Schedule created successfully",
            "data": {
                "id": schedule_id,
                "route_name": schedule.route_name,
                "collection_points": schedule.collection_points,
                "barangay": schedule.barangay,
                "day_of_week": schedule.day_of_week,
                "waste_type": schedule.waste_type,
                "time_slot": schedule.time_slot,
                "start_time": start_time,
                "end_time": end_time,
                "driver_id": schedule.driver_id,
                "assigned_driver_name": driver_name,
                "is_active": True,
            },
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/schedules/{schedule_id}")
async def update_schedule(schedule_id: str, request: dict, current_user: dict = Depends(require_admin)):
    """Update a schedule (day, waste type, time, active status, driver)."""
    db = SessionLocal()
    try:
        try:
            validate_schedule(request)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error))
        barangay = request.get("barangay")
        day_of_week = request.get("day_of_week")
        collection_type = request.get("collection_type")
        start_time = request.get("start_time")
        end_time = request.get("end_time")
        is_active = request.get("is_active")
        driver_id = request.get("driver_id")

        # Capture the previous values so an update that omits driver_id does
        # not silently unassign the driver, and so we only alert on real edits.
        existing = db.execute(
            text("""
                SELECT id, driver_id, barangay, day_of_week, start_time,
                       end_time, collection_type, is_active, route_name, collection_points
                FROM schedules WHERE id = :id
            """),
            {"id": schedule_id},
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Schedule not found")

        previous_driver_id = existing[1]
        if "driver_id" not in request:
            driver_id = previous_driver_id
        # Preserve fields the caller omitted instead of nulling them.
        if barangay is None:
            barangay = existing[2]
        if day_of_week is None:
            day_of_week = existing[3]
        if start_time is None:
            start_time = existing[4]
        if end_time is None:
            end_time = existing[5]
        if collection_type is None:
            collection_type = existing[6]
        if is_active is None:
            is_active = bool(existing[7])
        try:
            name = validate_route_name(request["route_name"]) if "route_name" in request else existing[8] or ""
            points = validate_points(request["collection_points"]) if "collection_points" in request else route_payload({"collection_points": existing[9]})["collection_points"]
            validate_driver(db, driver_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error))

        # Update the schedule
        update_query = """
            UPDATE schedules
            SET barangay = :barangay,
                day_of_week = :day_of_week,
                collection_type = :collection_type,
                start_time = :start_time,
                end_time = :end_time,
                is_active = :is_active,
                driver_id = :driver_id,
                route_name = :route_name,
                collection_points = :collection_points
            WHERE id = :id
        """
        db.execute(
            text(update_query),
            {
                "id": schedule_id,
                "barangay": barangay,
                "day_of_week": day_of_week,
                "collection_type": collection_type,
                "start_time": start_time,
                "end_time": end_time,
                "is_active": is_active,
                "driver_id": driver_id,
                "route_name": name,
                "collection_points": json.dumps(points),
            },
        )

        db.commit()

        shift_changed = (
            str(barangay) != str(existing[2])
            or str(day_of_week) != str(existing[3])
            or not _same_schedule_time(start_time, existing[4])
            or not _same_schedule_time(end_time, existing[5])
            or str(collection_type) != str(existing[6])
        )
        if driver_id and (
            str(driver_id) != str(previous_driver_id) or shift_changed
        ):
            days_map = {
                0: "Monday",
                1: "Tuesday",
                2: "Wednesday",
                3: "Thursday",
                4: "Friday",
                5: "Saturday",
                6: "Sunday",
            }
            try:
                day_idx = int(day_of_week)
            except (TypeError, ValueError):
                day_idx = -1
            day_name = days_map.get(day_idx, "Scheduled day")
            try:
                db.execute(
                    text("""INSERT INTO notifications 
                       (id, user_id, title, body, type, is_read, created_at)
                       VALUES (:id, :user_id, :title, :body, :type, FALSE, CURRENT_TIMESTAMP)"""),
                    {
                        "id": str(uuid.uuid4()),
                        "user_id": driver_id,
                        "title": "Shift updated",
                        "body": f"{barangay} • {day_name}, {start_time} - {end_time} ({collection_type or 'Collection'})",
                        "type": "shift",
                    },
                )
                db.commit()
            except Exception as notify_error:
                db.rollback()
                logging.warning(
                    f"Schedule updated but driver notification failed: {notify_error}"
                )

        # Get driver name for response
        driver_name = None
        if driver_id:
            driver = db.execute(
                text("SELECT full_name FROM users WHERE id = :id"),
                {"id": driver_id},
            ).fetchone()
            driver_name = driver[0] if driver else None

        days_map = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }
        day_name = days_map.get(day_of_week, "Unknown Day")

        return {
            "success": True,
            "message": "Schedule updated successfully",
            "data": {
                "id": schedule_id,
                "route_name": name,
                "collection_points": points,
                "barangay": barangay,
                "day_of_week": day_of_week,
                "waste_type": collection_type,
                "time_slot": f"{start_time} - {end_time}",
                "is_active": is_active,
                "driver_id": driver_id,
                "assigned_driver_name": driver_name,
            },
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str, current_user: dict = Depends(require_admin)):
    """Delete a schedule."""
    db = SessionLocal()
    try:
        # Check if schedule exists
        existing = db.execute(
            text("SELECT id FROM schedules WHERE id = :id"),
            {"id": schedule_id},
        ).fetchone()

        if not existing:
            raise HTTPException(status_code=404, detail="Schedule not found")

        # Delete the schedule
        db.execute(
            text("DELETE FROM schedules WHERE id = :id"),
            {"id": schedule_id},
        )

        db.commit()
        return {"success": True, "message": "Schedule deleted successfully"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/schedules/{schedule_id}/assign")
async def assign_driver_to_schedule(schedule_id: str, request: dict, current_user: dict = Depends(require_admin)):
    driver_id = request.get("driver_id")
    if not driver_id:
        raise HTTPException(status_code=400, detail="driver_id is required")

    db = SessionLocal()
    try:
        try:
            validate_driver(db, driver_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error))
        # 1. Get the schedule details
        schedule = db.execute(
            text("""
                SELECT barangay, day_of_week, start_time, end_time,
                       collection_type, driver_id
                FROM schedules WHERE id = :id
            """),
            {"id": schedule_id},
        ).fetchone()

        if not schedule:
            raise HTTPException(status_code=404, detail="Schedule not found")

        barangay = schedule[0]
        day_idx = schedule[1]
        start_time = schedule[2]
        end_time = schedule[3]
        collection_type = schedule[4]
        previous_driver_id = schedule[5]

        days_map = {
            0: "Monday",
            1: "Tuesday",
            2: "Wednesday",
            3: "Thursday",
            4: "Friday",
            5: "Saturday",
            6: "Sunday",
        }
        day_name = days_map.get(day_idx, "Unknown Day")

        # 2. Get the driver's name
        driver = db.execute(
            text("SELECT full_name FROM users WHERE id = :id"), {"id": driver_id}
        ).fetchone()
        driver_name = driver[0] if driver else "A driver"

        # 3. Assign the driver to the schedule
        db.execute(
            text("UPDATE schedules SET driver_id = :driver_id WHERE id = :id"),
            {"driver_id": driver_id, "id": schedule_id},
        )

        # 4. Notify all residents in that barangay
        residents = db.execute(
            text(
                "SELECT id FROM users WHERE barangay = :barangay AND role = 'resident'"
            ),
            {"barangay": barangay},
        ).fetchall()

        for resident in residents:
            db.execute(
                text("""INSERT INTO notifications 
                   (id, user_id, title, body, type, is_read, created_at)
                   VALUES (:id, :user_id, :title, :body, :type, FALSE, CURRENT_TIMESTAMP)"""),
                {
                    "id": str(uuid.uuid4()),
                    "user_id": resident[0],
                    "title": "Driver Assigned",
                    "body": f"{driver_name} has been assigned for garbage collection in {barangay} on {day_name}.",
                    "type": "schedule",
                },
            )

        # Tell the driver about the shift too. Re-assigning the same driver
        # does not create a duplicate alert.
        if str(previous_driver_id) != str(driver_id):
            db.execute(
                text("""INSERT INTO notifications 
                   (id, user_id, title, body, type, is_read, created_at)
                   VALUES (:id, :user_id, :title, :body, :type, FALSE, CURRENT_TIMESTAMP)"""),
                {
                    "id": str(uuid.uuid4()),
                    "user_id": driver_id,
                    "title": "New shift assigned",
                    "body": f"{barangay} • {day_name}, {start_time} - {end_time} ({collection_type or 'Collection'})",
                    "type": "shift",
                },
            )

        db.commit()
        return {"success": True, "message": "Driver assigned and residents notified."}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/drivers")
async def get_drivers(current_user: dict = Depends(get_current_user_record)):
    """Get list of available drivers for assignment."""
    db = SessionLocal()
    try:
        result = db.execute(
            text("""SELECT id, full_name, email, barangay, assigned_truck_plate 
                     FROM users 
                     WHERE role = 'driver' AND (is_active IS NULL OR is_active = TRUE)
                     ORDER BY full_name""")
        ).fetchall()

        drivers = []
        for row in result:
            mapping = row._mapping if hasattr(row, "_mapping") else None
            if mapping:
                drivers.append(
                    {
                        "id": mapping.get("id"),
                        "full_name": mapping.get("full_name"),
                        "email": mapping.get("email"),
                        "barangay": mapping.get("barangay"),
                        "assigned_truck_plate": mapping.get("assigned_truck_plate"),
                    }
                )
            else:
                drivers.append(
                    {
                        "id": row[0],
                        "full_name": row[1],
                        "email": row[2],
                        "barangay": row[3],
                        "assigned_truck_plate": row[4],
                    }
                )

        return {"success": True, "data": drivers}
    finally:
        db.close()


# ============= ANALYTICS (Admin) =============


import threading

_analytics_cache_lock = threading.Lock()
_analytics_cache = {"data": None, "timestamp": None, "refresh_task": None}
_ANALYTICS_CACHE_TTL = 30  # seconds


def _refresh_analytics_cache():
    """Background task to refresh analytics cache."""
    try:
        month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
        db = SessionLocal()
        try:
            # Consolidated query 1: users by role + counts
            users_counts = db.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM schedules WHERE is_active = TRUE) as active_schedules,
                    (SELECT COUNT(*) FROM truck_locations WHERE status = 'active') as active_trucks
            """)).fetchone()

            users_by_role = db.execute(text(
                "SELECT role, COUNT(*) as count FROM users GROUP BY role"
            )).fetchall()

            # Consolidated query 2: reports — status, month, top barangays
            reports_data = db.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM reports WHERE created_at >= :month_start) as reports_this_month,
                    (SELECT COUNT(*) FROM collection_records WHERE status = 'completed' AND collected_at >= :month_start) as collections_this_month
            """), {"month_start": month_start}).fetchone()

            reports_by_status = db.execute(text(
                "SELECT status, COUNT(*) as count FROM reports GROUP BY status"
            )).fetchall()

            top_barangays = db.execute(text(
                "SELECT barangay, COUNT(*) as count FROM reports GROUP BY barangay ORDER BY count DESC LIMIT 5"
            )).fetchall()

            # Consolidated query 3: collection_records — waste totals
            total_waste = db.execute(text(
                "SELECT SUM(waste_amount_kg) as total FROM collection_records WHERE status = 'completed'"
            )).fetchone()

            waste_by_type = db.execute(text(
                "SELECT waste_type, SUM(waste_amount_kg) as total FROM collection_records WHERE status = 'completed' AND waste_type IS NOT NULL GROUP BY waste_type"
            )).fetchall()

            data = {
                "users_by_role": [
                    {"role": row[0] or "unknown", "count": row[1]}
                    for row in users_by_role
                ],
                "reports_by_status": [
                    {"status": row[0] or "unknown", "count": row[1]}
                    for row in reports_by_status
                ],
                "total_waste_kg": float(total_waste[0] or 0) if total_waste else 0.0,
                "active_schedules": int(users_counts[0]) if users_counts else 0,
                "active_trucks": int(users_counts[1]) if users_counts else 0,
                "waste_by_type": [
                    {"type": row[0], "total_kg": float(row[1])} for row in waste_by_type
                ],
                "reports_this_month": int(reports_data[0]) if reports_data else 0,
                "collections_this_month": int(reports_data[1]) if reports_data else 0,
                "top_barangays": [
                    {"barangay": row[0], "reports": row[1]} for row in top_barangays
                ],
            }

            with _analytics_cache_lock:
                _analytics_cache["data"] = data
                _analytics_cache["timestamp"] = datetime.now()
        finally:
            db.close()
    except Exception as e:
        print(f"[AnalyticsCache] Background refresh failed: {e}")


@app.get("/api/analytics")
async def get_analytics(current_user: dict = Depends(get_current_user_record)):
    """Get system analytics for admin dashboard. Cached 30s with background refresh."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    with _analytics_cache_lock:
        cached_data = _analytics_cache["data"]
        cached_ts = _analytics_cache["timestamp"]

    now = datetime.now()
    if cached_data is not None and cached_ts is not None:
        age = (now - cached_ts).total_seconds()
        if age < _ANALYTICS_CACHE_TTL:
            if age > (_ANALYTICS_CACHE_TTL // 2) and _analytics_cache["refresh_task"] is None:
                _analytics_cache["refresh_task"] = threading.Thread(
                    target=_refresh_analytics_cache, daemon=True
                )
                _analytics_cache["refresh_task"].start()
                _analytics_cache["refresh_task"] = None
            return {"success": True, "data": cached_data}

    # Cold path: fetch fresh data
    month_start = now.replace(day=1).strftime('%Y-%m-%d')
    db = SessionLocal()
    try:
        users_by_role = db.execute(text(
            "SELECT role, COUNT(*) as count FROM users GROUP BY role"
        )).fetchall()

        reports_by_status = db.execute(text(
            "SELECT status, COUNT(*) as count FROM reports GROUP BY status"
        )).fetchall()

        total_waste = db.execute(text(
            "SELECT SUM(waste_amount_kg) as total FROM collection_records WHERE status = 'completed'"
        )).fetchone()

        active_schedules = db.execute(text(
            "SELECT COUNT(*) as count FROM schedules WHERE is_active = TRUE"
        )).fetchone()

        active_trucks = db.execute(text(
            "SELECT COUNT(*) as count FROM truck_locations WHERE status = 'active'"
        )).fetchone()

        waste_by_type = db.execute(text(
            "SELECT waste_type, SUM(waste_amount_kg) as total FROM collection_records WHERE status = 'completed' AND waste_type IS NOT NULL GROUP BY waste_type"
        )).fetchall()

        reports_this_month = db.execute(text(
            "SELECT COUNT(*) as count FROM reports WHERE created_at >= :month_start"
        ), {"month_start": month_start}).fetchone()

        collections_this_month = db.execute(text(
            "SELECT COUNT(*) as count FROM collection_records WHERE status = 'completed' AND collected_at >= :month_start"
        ), {"month_start": month_start}).fetchone()

        top_barangays = db.execute(text(
            "SELECT barangay, COUNT(*) as count FROM reports GROUP BY barangay ORDER BY count DESC LIMIT 5"
        )).fetchall()

        data = {
            "users_by_role": [
                {"role": row[0] or "unknown", "count": row[1]}
                for row in users_by_role
            ],
            "reports_by_status": [
                {"status": row[0] or "unknown", "count": row[1]}
                for row in reports_by_status
            ],
            "total_waste_kg": float(total_waste[0] or 0) if total_waste else 0.0,
            "active_schedules": int(active_schedules[0]) if active_schedules else 0,
            "active_trucks": int(active_trucks[0]) if active_trucks else 0,
            "waste_by_type": [
                {"type": row[0], "total_kg": float(row[1])} for row in waste_by_type
            ],
            "reports_this_month": int(reports_this_month[0]) if reports_this_month else 0,
            "collections_this_month": int(collections_this_month[0]) if collections_this_month else 0,
            "top_barangays": [
                {"barangay": row[0], "reports": row[1]} for row in top_barangays
            ],
        }

        with _analytics_cache_lock:
            _analytics_cache["data"] = data
            _analytics_cache["timestamp"] = datetime.now()

        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= MANUAL WASTE ENTRIES (Admin) =============


@app.post("/api/admin/waste-entries")
async def create_waste_entry(
    request: dict, current_user: dict = Depends(require_admin)
):
    """Record completed waste that did not come from a driver route.

    Driver collections are written by /api/collections/{id}/collected. This
    endpoint lets an admin keep the Total Waste counter current when waste is
    dropped off at the office or tallied from a paper log.
    """
    barangay = str(request.get("barangay") or "").strip()
    waste_type = str(
        request.get("waste_type") or request.get("category") or ""
    ).strip()
    try:
        total_kg = float(
            request.get("waste_amount_kg") or request.get("weight_kg") or 0
        )
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Weight must be a number")

    if not barangay or not waste_type or total_kg <= 0:
        raise HTTPException(
            status_code=400,
            detail="Barangay, waste type and a positive weight are required",
        )

    date_str = str(
        request.get("date") or get_ph_time().strftime("%Y-%m-%d")
    ).strip()
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")

    normalized = waste_type.lower().replace("_", " ").strip()
    biodegradable_kg = total_kg if normalized in ("biodegradable", "bio") else 0
    recyclable_kg = total_kg if normalized in ("recyclable", "recycle") else 0
    non_biodegradable_kg = (
        total_kg
        if normalized in ("non-biodegradable", "non biodegradable", "residual")
        else 0
    )
    special_waste_kg = (
        total_kg
        if "hazard" in normalized or normalized.startswith("special")
        else 0
    )

    record_id = str(uuid.uuid4())
    notes = request.get("notes")
    db = SessionLocal()
    try:
        db.execute(
            text("""
                INSERT INTO collection_records
                (id, driver_id, schedule_id, barangay, waste_type, time_slot,
                 date, status, waste_amount_kg, biodegradable_kg,
                 non_biodegradable_kg, recyclable_kg, special_waste_kg,
                 notes, collected_at)
                VALUES
                (:id, :driver_id, :schedule_id, :barangay, :waste_type, NULL, :date,
                 'completed', :waste_amount, :biodegradable,
                 :non_biodegradable, :recyclable, :special_waste,
                 :notes, CURRENT_TIMESTAMP)
            """),
            {
                "id": record_id,
                "driver_id": MANUAL_ENTRY_DRIVER_ID,
                "schedule_id": MANUAL_ENTRY_SCHEDULE_ID,
                "barangay": barangay,
                "waste_type": waste_type,
                "date": date_str,
                "waste_amount": total_kg,
                "biodegradable": biodegradable_kg,
                "non_biodegradable": non_biodegradable_kg,
                "recyclable": recyclable_kg,
                "special_waste": special_waste_kg,
                "notes": notes,
            },
        )
        db.commit()

        # The dashboard caches analytics for 30 seconds; clear it so the new
        # total is visible on the next refresh.
        with _analytics_cache_lock:
            _analytics_cache["data"] = None
            _analytics_cache["timestamp"] = None

        total_row = db.execute(
            text("""
                SELECT COALESCE(SUM(waste_amount_kg), 0)
                FROM collection_records
                WHERE status = 'completed'
            """)
        ).fetchone()
        total_kg_all = float(total_row[0] or 0) if total_row else 0.0

        return {
            "success": True,
            "message": "Waste entry recorded",
            "data": {
                "id": record_id,
                "total_waste_kg": total_kg_all,
            },
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/admin/waste-entries")
async def list_waste_entries(
    limit: int = 20, current_user: dict = Depends(require_admin)
):
    """Recent manual waste entries, newest first."""
    limit, _ = _normalize_pagination(limit, 0, default_limit=20, max_limit=100)
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT id, barangay, waste_type, waste_amount_kg, date, notes,
                       created_at
                FROM collection_records
                WHERE status = 'completed' AND driver_id = :manual_driver_id
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"limit": limit, "manual_driver_id": MANUAL_ENTRY_DRIVER_ID},
        ).fetchall()
        return {
            "success": True,
            "data": [
                {
                    "id": row[0],
                    "barangay": row[1],
                    "waste_type": row[2],
                    "waste_amount_kg": float(row[3] or 0),
                    "date": str(row[4]) if row[4] else None,
                    "notes": row[5],
                    "created_at": str(row[6]) if row[6] else None,
                }
                for row in rows
            ],
        }
    finally:
        db.close()


@app.get("/api/driver/my-schedules")
async def get_driver_schedules(
    current_user: dict = Depends(get_current_user_record), today: bool = False
):
    """Get schedules assigned to the current driver. Use ?today=true to filter only today's schedule."""
    user_id = current_user.get("id")
    user_role = current_user.get("role", "resident")

    if user_role != "driver":
        raise HTTPException(
            status_code=403, detail="Only drivers can access this endpoint"
        )

    db = SessionLocal()
    try:
        query = """SELECT s.*, u.full_name as assigned_driver_name
                   FROM schedules s
                   LEFT JOIN users u ON s.driver_id = u.id
                   WHERE s.driver_id = :user_id AND s.driver_id IS NOT NULL AND s.is_active = TRUE"""

        params = {"user_id": user_id}
        print(f"[DEBUG] get_driver_schedules - user_id={user_id}, role={user_role}")

        if today:
            current_day = get_ph_weekday()
            query += " AND s.day_of_week = :day_of_week"
            params["day_of_week"] = current_day

        query += " ORDER BY s.day_of_week, s.start_time"

        result = db.execute(text(query), params).fetchall()
        print(f"[DEBUG] get_driver_schedules - returned {len(result)} schedules")

        schedules = []
        for row in result:
            mapping = row._mapping
            schedule_id = mapping.get("id")
            schedule_driver_id = mapping.get("driver_id")

            # Defensive check: only include schedules matching the current driver
            if schedule_driver_id and schedule_driver_id == user_id:
                schedules.append(
                    {
                        "id": schedule_id,
                        **route_payload(mapping),
                        "barangay": mapping.get("barangay"),
                        "day_of_week": mapping.get("day_of_week", 0),
                        "day_name": _get_day_name(mapping.get("day_of_week", 0)),
                        "waste_type": mapping.get("collection_type", ""),
                        "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                        "start_time": mapping.get("start_time"),
                        "end_time": mapping.get("end_time"),
                        "is_active": bool(mapping.get("is_active", True)),
                        "driver_id": schedule_driver_id,
                        "assigned_driver_name": mapping.get("assigned_driver_name"),
                    }
                )
            else:
                print(
                    f"[DEBUG] SKIPPED schedule {schedule_id} - driver_id mismatch: {schedule_driver_id} != {user_id}"
                )

        print(f"[DEBUG] get_driver_schedules - final schedules count: {len(schedules)}")
        return {"success": True, "data": schedules}
    finally:
        db.close()


@app.put("/api/driver/status")
async def update_driver_status(current_user: dict = Depends(get_current_user_record)):
    """Update driver online status and location sharing."""
    user_id = current_user.get("id")
    user_role = current_user.get("role", "resident")

    if user_role != "driver":
        raise HTTPException(
            status_code=403, detail="Only drivers can access this endpoint"
        )

    db = SessionLocal()
    try:
        # Get the driver's truck info by user_id (full UUID)
        truck = db.execute(
            text("SELECT * FROM truck_locations WHERE truck_id = :user_id"),
            {"user_id": user_id},
        ).fetchone()
        if not truck:
            truck = db.execute(
                text("SELECT * FROM truck_locations WHERE driver_name = :name LIMIT 1"),
                {"name": current_user.get("full_name")},
            ).fetchone()

        if truck:
            # Update truck status. is_on_duty records the driver's intent so a
            # GPS ping or the stale sweeper can never silently end the shift.
            db.execute(
                text(
                    "UPDATE truck_locations SET status = 'active', is_on_duty = TRUE, "
                    "driver_name = :name, last_updated = CURRENT_TIMESTAMP "
                    "WHERE truck_id = :user_id"
                ),
                {"user_id": user_id, "name": current_user.get("full_name")},
            )
        else:
            # The driver may already have a location row keyed to a legacy
            # DRIVER_<first8> id; claim it instead of creating a ghost truck.
            legacy = db.execute(
                text("SELECT id FROM truck_locations WHERE truck_id = :legacy LIMIT 1"),
                {"legacy": f"DRIVER_{str(user_id)[:8]}"},
            ).fetchone()
            if legacy:
                db.execute(
                    text(
                        "UPDATE truck_locations SET truck_id = :user_id, "
                        "status = 'active', is_on_duty = TRUE, driver_name = :name, "
                        "last_updated = CURRENT_TIMESTAMP WHERE id = :id"
                    ),
                    {
                        "id": legacy[0],
                        "user_id": user_id,
                        "name": current_user.get("full_name"),
                    },
                )
            else:
                # Create new truck entry for this driver
                db.execute(
                    text("""INSERT INTO truck_locations
                       (id, truck_id, driver_name, plate_number, latitude, longitude,
                        current_barangay, status, speed, last_updated, is_on_duty)
                       VALUES (:id, :truck_id, :driver_name, 'PENDING', 0, 0,
                               'Unknown', 'active', 0, CURRENT_TIMESTAMP, TRUE)"""),
                    {
                        "id": str(uuid.uuid4()),
                        "truck_id": user_id,
                        "driver_name": current_user.get("full_name"),
                    },
                )

        db.commit()
        return {"success": True, "message": "Driver is now online"}
    finally:
        db.close()


@app.put("/api/driver/go-offline")
async def driver_go_offline(current_user: dict = Depends(get_current_user_record)):
    """Set driver as offline (stop sharing location)."""
    user_id = current_user.get("id")
    user_role = current_user.get("role", "resident")

    if user_role != "driver":
        raise HTTPException(
            status_code=403, detail="Only drivers can access this endpoint"
        )

    db = SessionLocal()
    try:
        db.execute(
            text(
                "UPDATE truck_locations SET status = 'offline', is_on_duty = FALSE, "
                "last_updated = CURRENT_TIMESTAMP WHERE truck_id = :user_id"
            ),
            {"user_id": user_id},
        )
        db.commit()
        return {"success": True, "message": "Driver is now offline"}
    finally:
        db.close()


def _get_day_name(day_num):
    days = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    # schedules.day_of_week is 0=Monday ... 6=Sunday (see get_ph_weekday()).
    try:
        index = int(day_num)
    except (TypeError, ValueError):
        return "Unknown"
    if 0 <= index <= 6:
        return days[index]
    return "Unknown"


# ============= WASTE SCAN ACTIVITY =============


@app.post("/api/waste-scan")
async def submit_waste_scan(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Record a validated waste scan and award points."""
    user_id = current_user.get("id")
    category = request.get("category", "")
    confidence = request.get("confidence", 0.0)
    detected_class = request.get("detected_class", "")
    points_to_award = 5

    if not category or category == "Not Waste":
        return {"success": False, "message": "Invalid waste scan", "points_awarded": 0}

    db = SessionLocal()
    try:
        # Ensure scan_activities table exists
        try:
            db.execute(
                text("""
                    CREATE TABLE IF NOT EXISTS scan_activities (
                        id VARCHAR(36) PRIMARY KEY,
                        user_id VARCHAR(36) NOT NULL,
                        category VARCHAR(100),
                        detected_class VARCHAR(100),
                        confidence FLOAT DEFAULT 0,
                        points_awarded INT DEFAULT 0,
                        scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            )
            db.commit()
        except:
            pass

        # Rate limit: max 10 scans per day
        today_date = get_ph_time().strftime("%Y-%m-%d")
        try:
            count_result = db.execute(
                text(
                    "SELECT COUNT(*) FROM scan_activities WHERE user_id = :uid AND DATE(scanned_at) = :today"
                ),
                {"uid": user_id, "today": today_date},
            ).fetchone()
            daily_count = count_result[0] if count_result else 0
        except:
            daily_count = 0

        if daily_count >= 10:
            return {
                "success": True,
                "message": "Daily scan limit reached (10/day). Come back tomorrow!",
                "points_awarded": 0,
                "daily_count": daily_count,
            }

        # Record scan
        scan_id = str(uuid.uuid4())
        db.execute(
            text("""
                INSERT INTO scan_activities (id, user_id, category, detected_class, confidence, points_awarded)
                VALUES (:id, :uid, :cat, :cls, :conf, :pts)
            """),
            {
                "id": scan_id,
                "uid": user_id,
                "cat": category,
                "cls": detected_class,
                "conf": confidence,
                "pts": points_to_award,
            },
        )

        # Award points
        try:
            db.execute(
                text(
                    "UPDATE users SET total_points = COALESCE(total_points, 0) + :pts, lifetime_points = COALESCE(lifetime_points, 0) + :pts WHERE id = :uid"
                ),
                {"pts": points_to_award, "uid": user_id},
            )
        except:
            pass

        db.commit()
        return {
            "success": True,
            "message": f"Waste identified as {category}! +{points_to_award} points",
            "points_awarded": points_to_award,
            "daily_count": daily_count + 1,
            "scan_id": scan_id,
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/waste-scan/correction")
async def submit_waste_scan_correction(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    """Store a user's correction so the scanner's mistakes can be reviewed.

    This does not change points that were already awarded by /api/waste-scan.
    It is a feedback log, not a training upload: no photo is sent here.
    """
    user_id = current_user.get("id")
    allowed_categories = {
        "Biodegradable",
        "Recyclable",
        "Residual",
        "Non-Biodegradable",
        "Hazardous",
        "Not Waste",
    }
    predicted = str(request.get("predicted_category") or "").strip()
    corrected = str(request.get("corrected_category") or "").strip()
    if corrected not in allowed_categories:
        raise HTTPException(status_code=400, detail="Unknown corrected category")

    try:
        confidence = float(request.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    scan_id = str(request.get("scan_id") or "").strip() or None

    db = SessionLocal()
    try:
        db.execute(
            text("""
                CREATE TABLE IF NOT EXISTS scan_corrections (
                    id VARCHAR(36) PRIMARY KEY,
                    scan_id VARCHAR(36) NULL,
                    user_id VARCHAR(36) NOT NULL,
                    predicted_category VARCHAR(100),
                    corrected_category VARCHAR(100) NOT NULL,
                    confidence FLOAT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        )

        existing = None
        if scan_id:
            existing = db.execute(
                text("""
                    SELECT id FROM scan_corrections
                    WHERE scan_id = :scan_id AND user_id = :user_id
                    LIMIT 1
                """),
                {"scan_id": scan_id, "user_id": user_id},
            ).fetchone()

        if existing:
            db.execute(
                text("""
                    UPDATE scan_corrections
                    SET predicted_category = :predicted,
                        corrected_category = :corrected,
                        confidence = :confidence,
                        created_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """),
                {
                    "predicted": predicted,
                    "corrected": corrected,
                    "confidence": confidence,
                    "id": existing[0],
                },
            )
            correction_id = existing[0]
            updated = True
        else:
            correction_id = str(uuid.uuid4())
            db.execute(
                text("""
                    INSERT INTO scan_corrections
                    (id, scan_id, user_id, predicted_category,
                     corrected_category, confidence)
                    VALUES
                    (:id, :scan_id, :user_id, :predicted, :corrected, :confidence)
                """),
                {
                    "id": correction_id,
                    "scan_id": scan_id,
                    "user_id": user_id,
                    "predicted": predicted,
                    "corrected": corrected,
                    "confidence": confidence,
                },
            )
            updated = False

        db.commit()
        return {
            "success": True,
            "message": "Correction saved for review",
            "data": {
                "id": correction_id,
                "updated": updated,
            },
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= BARANGAY SCORES (Gamification) =============


@app.get("/api/barangay-scores")
async def get_barangay_scores(response: Response = None):
    db = SessionLocal()
    try:
        # Calculate from users table, focusing ONLY on residents to be fair.
        result = db.execute(
            text("""
                SELECT 
                    barangay as barangay_name,
                    SUM(total_points) as total_points,
                    SUM(reports_submitted) as total_reports,
                    COUNT(id) as resident_count
                FROM users
                WHERE role = 'resident' AND barangay IS NOT NULL
                GROUP BY barangay
            """)
        ).fetchall()

        scores = []
        for row in result:
            m = row._mapping
            total_points = int(m.get("total_points", 0) or 0)
            resident_count = int(m.get("resident_count", 0) or 0)
            points_per_resident = total_points / max(1, resident_count)
            
            scores.append(
                {
                    "barangay_name": m.get("barangay_name"),
                    "total_points": total_points,
                    "total_reports": int(m.get("total_reports", 0) or 0),
                    "resident_count": resident_count,
                    "points_per_resident": points_per_resident
                }
            )
            
        # Sort by fair points_per_resident metric descending
        scores.sort(key=lambda x: x["points_per_resident"], reverse=True)
        
        # Add rank
        for i, score in enumerate(scores):
            score["rank"] = i + 1

        # Cache control
        if response:
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

        return scores
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= REWARDS & REDEMPTIONS =============


@app.get("/api/rewards")
async def get_rewards(response: Response = None):
    db = SessionLocal()
    try:
        result = db.execute(
            text(
                "SELECT * FROM rewards WHERE is_active = TRUE ORDER BY points_required ASC"
            )
        ).fetchall()

        rewards = []
        for row in result:
            m = row._mapping
            rewards.append(
                {
                    "id": m.get("id"),
                    "name": m.get("name"),
                    "description": m.get("description"),
                    "points_required": m.get("points_required"),
                    "image_url": m.get("image_url"),
                    "available_quantity": m.get("available_quantity"),
                    "is_active": m.get("is_active"),
                }
            )
        if response:
            response.headers["Cache-Control"] = "public, max-age=600"
        return {"success": True, "data": rewards}
    finally:
        db.close()


@app.post("/api/redemptions")
async def create_redemption(request: dict, current_user: dict = Depends(get_current_user_record)):
    user_id = request.get("user_id") or request.get("userId")
    # Only admins may redeem on behalf of another user.
    if current_user.get("role") != "admin" or not user_id:
        user_id = current_user.get("id")
    reward_id = request.get("reward_id") or request.get("rewardId")

    if not user_id or not reward_id:
        raise HTTPException(
            status_code=400, detail="user_id and reward_id are required"
        )

    db = SessionLocal()
    try:
        # Get reward details
        reward = db.execute(
            text("SELECT * FROM rewards WHERE id = :id"), {"id": reward_id}
        ).fetchone()

        if not reward:
            raise HTTPException(status_code=404, detail="Reward not found")

        reward_map = reward._mapping
        points_required = reward_map.get("points_required", 0)

        # Check user points
        user = db.execute(
            text("SELECT * FROM users WHERE id = :id"), {"id": user_id}
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user_map = user._mapping
        user_points = user_map.get("total_points", 0) or 0

        if user_points < points_required:
            raise HTTPException(status_code=400, detail="Insufficient points")

        # Deduct points and create redemption
        redemption_id = str(uuid.uuid4())
        db.execute(
            text("""
                INSERT INTO redemptions (id, user_id, reward_id, reward_name, points_spent, status)
                VALUES (:id, :uid, :rid, :rname, :points, 'pending')
            """),
            {
                "id": redemption_id,
                "uid": user_id,
                "rid": reward_id,
                "rname": reward_map.get("name"),
                "points": points_required,
            },
        )

        # Deduct points from user
        db.execute(
            text(
                "UPDATE users SET total_points = total_points - :points WHERE id = :uid"
            ),
            {"points": points_required, "uid": user_id},
        )

        db.commit()
        return {
            "success": True,
            "message": "Redemption successful",
            "data": {"id": redemption_id},
        }
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/redemptions")
async def get_redemptions(userId: str = None, limit: int = 100, offset: int = 0, current_user: dict = Depends(get_current_user_record)):
    # Residents only see their own redemptions; admins can query anyone.
    if current_user.get("role") != "admin":
        userId = current_user.get("id")
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=200)
        if userId:
            result = db.execute(
                text(
                    "SELECT * FROM redemptions WHERE user_id = :uid ORDER BY redeemed_at DESC LIMIT :limit OFFSET :offset"
                ),
                {"uid": userId, "limit": limit, "offset": offset},
            ).fetchall()
        else:
            result = db.execute(
                text("SELECT * FROM redemptions ORDER BY redeemed_at DESC LIMIT :limit OFFSET :offset"),
                {"limit": limit, "offset": offset},
            ).fetchall()

        redemptions = []
        for row in result:
            m = row._mapping
            redemptions.append(
                {
                    "id": m.get("id"),
                    "user_id": m.get("user_id"),
                    "reward_id": m.get("reward_id"),
                    "reward_name": m.get("reward_name"),
                    "points_spent": m.get("points_spent"),
                    "status": m.get("status"),
                    "redeemed_at": str(m.get("redeemed_at"))
                    if m.get("redeemed_at")
                    else None,
                    "processed_at": str(m.get("processed_at"))
                    if m.get("processed_at")
                    else None,
                }
            )
        return {"success": True, "data": redemptions}
    finally:
        db.close()


@app.put("/api/redemptions/{redemption_id}")
async def update_redemption_status(redemption_id: str, request: dict, current_user: dict = Depends(require_admin)):
    status = request.get("status")
    if not status:
        raise HTTPException(status_code=400, detail="status is required")

    db = SessionLocal()
    try:
        db.execute(
            text(
                "UPDATE redemptions SET status = :status, processed_at = CURRENT_TIMESTAMP WHERE id = :id"
            ),
            {"status": status, "id": redemption_id},
        )
        db.commit()
        return {"success": True, "message": "Redemption updated"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= SURVEYS =============


@app.get("/api/surveys")
async def get_surveys(limit: int = 100, offset: int = 0):
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=200)
        result = db.execute(
            text(
                "SELECT * FROM surveys WHERE is_active = TRUE ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        surveys = []
        for row in result:
            m = row._mapping
            surveys.append(
                {
                    "id": m.get("id"),
                    "title": m.get("title"),
                    "description": m.get("description"),
                    "questions": m.get("questions"),
                    "target_barangay": m.get("target_barangay"),
                    "is_active": m.get("is_active"),
                    "created_at": str(m.get("created_at"))
                    if m.get("created_at")
                    else None,
                }
            )
        return {"success": True, "data": surveys}
    finally:
        db.close()


@app.post("/api/surveys")
async def submit_survey(request: dict, current_user: dict = Depends(get_current_user_record)):
    survey_id = request.get("survey_id") or request.get("surveyId")
    user_id = request.get("user_id") or request.get("userId")
    if current_user.get("role") != "admin" or not user_id:
        user_id = current_user.get("id")
    responses = request.get("responses")
    barangay = request.get("barangay")
    rating = request.get("rating")
    comment = request.get("comment")

    if not user_id or not responses:
        raise HTTPException(
            status_code=400, detail="user_id and responses are required"
        )

    db = SessionLocal()
    try:
        response_id = str(uuid.uuid4())

        # If rating/comment provided, treat as simple feedback
        if rating:
            db.execute(
                text("""
                    INSERT INTO survey_responses (id, survey_id, user_id, responses)
                    VALUES (:id, :sid, :uid, :responses)
                """),
                {
                    "id": response_id,
                    "sid": survey_id or "general",
                    "uid": user_id,
                    "responses": json.dumps(
                        {"rating": rating, "comment": comment, "barangay": barangay}
                    ),
                },
            )
        else:
            db.execute(
                text("""
                    INSERT INTO survey_responses (id, survey_id, user_id, responses)
                    VALUES (:id, :sid, :uid, :responses)
                """),
                {
                    "id": response_id,
                    "sid": survey_id or "general",
                    "uid": user_id,
                    "responses": json.dumps(responses),
                },
            )

        # Award points for completing survey
        try:
            db.execute(
                text(
                    "UPDATE users SET total_points = total_points + 10 WHERE id = :uid"
                ),
                {"uid": user_id},
            )
        except:
            pass  # Points column might not exist

        db.commit()
        return {
            "success": True,
            "message": "Survey submitted",
            "data": {"id": response_id},
        }
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/surveys/completed-this-week")
async def check_survey_completed_this_week(userId: str, all_time: bool = False):
    db = SessionLocal()
    try:
        if all_time:
            result = db.execute(
                text("SELECT id FROM survey_responses WHERE user_id = :uid LIMIT 1"),
                {"uid": userId},
            ).fetchone()
        else:
            cutoff_7d = get_ph_time().replace(tzinfo=None) - timedelta(days=7)
            result = db.execute(
                text("""
                    SELECT id FROM survey_responses 
                    WHERE user_id = :uid 
                    AND submitted_at >= :cutoff
                    LIMIT 1
                """),
                {"uid": userId, "cutoff": cutoff_7d},
            ).fetchone()

        return {"success": True, "completed": result is not None}
    finally:
        db.close()


@app.post("/api/rewards")
async def create_reward(request: dict, current_user: dict = Depends(require_admin)):
    db = SessionLocal()
    try:
        reward_id = str(uuid.uuid4())
        db.execute(
            text("""
                INSERT INTO rewards (id, name, description, points_required, image_url, available_quantity, is_active)
                VALUES (:id, :name, :desc, :points, :img, :qty, :active)
            """),
            {
                "id": reward_id,
                "name": request.get("name"),
                "desc": request.get("description"),
                "points": request.get("points_required", 100),
                "img": request.get("image_url"),
                "qty": request.get("available_quantity"),
                "active": True,
            },
        )
        db.commit()
        return {"success": True, "data": {"id": reward_id}}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/rewards/{reward_id}")
async def delete_reward(reward_id: str, current_user: dict = Depends(require_admin)):
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM rewards WHERE id = :id"), {"id": reward_id})
        db.commit()
        return {"success": True, "message": "Reward deleted"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


# ============= COLLECTIONS (Driver Routes) =============


@app.get("/api/collections/today")
async def get_collections_today():
    db = SessionLocal()
    try:
        today = get_ph_weekday()
        today_date = get_ph_time().strftime("%Y-%m-%d")
        result = db.execute(
            text("""SELECT s.id, s.route_name, s.collection_points, s.barangay, s.collection_type, s.start_time, s.end_time,
                           s.driver_id, s.is_active, u.full_name as driver_name,
                           COALESCE(cr.status, 'pending') as collection_status,
                           COALESCE(cr.waste_amount_kg, 0) as waste_amount
                    FROM schedules s
                    LEFT JOIN users u ON s.driver_id = u.id
                    LEFT JOIN collection_records cr ON cr.schedule_id = s.id AND DATE(cr.date) = :today_date
                    WHERE s.day_of_week = :today AND s.is_active = TRUE
                    ORDER BY s.start_time"""),
            {"today": today, "today_date": today_date},
        ).fetchall()

        collections = [
            {
                "id": str(uuid.uuid4()),
                **route_payload(row._mapping),
                "schedule_id": row._mapping.get("id"),
                "barangay": row._mapping.get("barangay"),
                "waste_type": row._mapping.get("collection_type", ""),
                "time_slot": f"{row._mapping.get('start_time', '')} - {row._mapping.get('end_time', '')}",
                "status": row._mapping.get("collection_status", "pending"),
                "driver_id": row._mapping.get("driver_id"),
                "driver_name": row._mapping.get("driver_name"),
                "waste_amount_kg": float(row._mapping.get("waste_amount", 0) or 0),
            }
            for row in result
        ]
        return {"success": True, "data": collections}
    except Exception as e:
        print(f"ERROR in get_collections_today: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/collections/start-route")
async def start_collection_route(
    request: dict, current_user: dict = Depends(get_current_user_record)
):
    db = SessionLocal()
    try:
        driver_id = request.get("driver_id") or current_user.get("id")
        if current_user.get("role") not in ("driver", "admin"):
            raise HTTPException(status_code=403, detail="Driver access required")
        if current_user.get("role") != "admin" and driver_id != current_user.get("id"):
            raise HTTPException(status_code=403, detail="Cannot access another driver's route")
        today = get_ph_weekday()
        today_date = datetime.now().strftime("%Y-%m-%d")

        result = db.execute(
            text("""SELECT s.*, u.full_name as driver_name 
                    FROM schedules s 
                    LEFT JOIN users u ON s.driver_id = u.id
                    WHERE s.day_of_week = :today AND s.is_active = TRUE
                    AND (s.driver_id = :driver_id OR EXISTS (
                        SELECT 1 FROM collection_records cr
                        WHERE cr.schedule_id = s.id AND cr.date = :today_date
                        AND cr.driver_id = :driver_id))
                    ORDER BY s.start_time"""),
            {"today": today, "driver_id": driver_id, "today_date": today_date},
        ).fetchall()

        records = []
        for row in result:
            mapping = row._mapping
            schedule_id = mapping.get("id")

            # Check if there's an existing collection record for TODAY
            status = "pending"
            # Deterministic per schedule per day, so repeated start-route calls
            # and double-submits hit the same collection_records row.
            existing_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"etrak:{schedule_id}:{today_date}"))
            try:
                existing = db.execute(
                    text("""SELECT id, status, driver_id FROM collection_records 
                            WHERE schedule_id = :schedule_id AND DATE(date) = :today_date
                            ORDER BY collected_at DESC LIMIT 1"""),
                    {"schedule_id": schedule_id, "today_date": today_date},
                ).fetchone()
                if existing:
                    if existing[2] and existing[2] != driver_id:
                        continue
                    existing_id = existing[0]
                    status = existing[1] or "pending"
            except Exception as e:
                print(f"Error checking existing: {e}")

            records.append(
                {
                    "id": existing_id,
                    **route_payload(mapping),
                    "schedule_id": schedule_id,
                    "barangay": mapping.get("barangay"),
                    "waste_type": mapping.get("collection_type", ""),
                    "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                    "status": status,
                    "driver_id": driver_id,
                    "driver_name": mapping.get("driver_name"),
                }
            )
        return {"success": True, "data": records}
    finally:
        db.close()


def _owned_collection_schedule(db, schedule_id, user):
    if user.get("role") not in ("driver", "admin"):
        raise HTTPException(status_code=403, detail="Driver access required")
    schedule = db.execute(text("SELECT * FROM schedules WHERE id = :id"),
                          {"id": schedule_id}).mappings().first()
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    record = db.execute(text("""SELECT * FROM collection_records
        WHERE schedule_id = :id AND date = :today"""),
        {"id": schedule_id, "today": get_ph_time().strftime("%Y-%m-%d")}).mappings().first()
    owner = record["driver_id"] if record and record["driver_id"] else schedule["driver_id"]
    if user.get("role") != "admin" and owner != user.get("id"):
        raise HTTPException(status_code=403, detail="Assignment belongs to another driver")
    if schedule["day_of_week"] != get_ph_weekday() or not schedule["is_active"]:
        raise HTTPException(status_code=409, detail="Schedule is not active today")
    return schedule, record


def _validate_collection_id(record_id, schedule_id, record):
    today = get_ph_time().strftime("%Y-%m-%d")
    expected = record["id"] if record else str(uuid.uuid5(
        uuid.NAMESPACE_URL, f"etrak:{schedule_id}:{today}"))
    if record_id != expected:
        raise HTTPException(status_code=409, detail="Reload today's route first")


@app.put("/api/collections/{record_id}/pause")
async def pause_collection(record_id: str, request: dict,
                           current_user: dict = Depends(get_current_user_record)):
    db = SessionLocal()
    try:
        schedule_id = request.get("schedule_id")
        schedule, record = _owned_collection_schedule(db, schedule_id, current_user)
        today = get_ph_time().strftime("%Y-%m-%d")
        expected_id = record["id"] if record else str(uuid.uuid5(
            uuid.NAMESPACE_URL, f"etrak:{schedule_id}:{today}"))
        if record_id != expected_id:
            raise HTTPException(status_code=409, detail="Reload today's route first")
        if record and record["status"] in ("completed", "collected", "skipped"):
            raise HTTPException(status_code=409, detail="Assignment already finished")
        if record and record["status"] == "paused":
            return {"success": True}
        if record:
            result = db.execute(text("""UPDATE collection_records
                SET status='paused', skip_reason='Truck full'
                WHERE id=:id AND (status IS NULL OR status NOT IN
                    ('paused', 'completed', 'collected', 'skipped'))"""),
                {"id": record_id})
            if result.rowcount != 1:
                raise HTTPException(status_code=409, detail="Assignment changed; refresh first")
        else:
            db.execute(text("""INSERT INTO collection_records
                (id, schedule_id, driver_id, barangay, waste_type, time_slot, date, status, skip_reason)
                VALUES (:id, :sid, :driver, :barangay, :waste, :slot, :today, 'paused', 'Truck full')"""),
                {"id": record_id, "sid": schedule_id, "driver": schedule["driver_id"],
                 "barangay": schedule["barangay"], "waste": schedule["collection_type"],
                 "slot": f"{schedule['start_time']} - {schedule['end_time']}", "today": today})
        driver_id = record["driver_id"] if record else schedule["driver_id"]
        db.execute(text("""INSERT INTO notifications
            (id, user_id, title, body, type, is_read,
             target_scope, target_role, schedule_id, created_at)
            VALUES (:id, NULL, :title, :body, 'truck_full', FALSE,
                    'role', 'admin', :schedule_id, CURRENT_TIMESTAMP)"""),
            {"id": str(uuid.uuid4()), "title": "Truck full — collection paused",
             "body": (f"{schedule['barangay']} • {today}, "
                      f"{schedule['start_time']} - {schedule['end_time']} "
                      f"({schedule['collection_type']}). Driver: {driver_id}. "
                      f"Collection: {record_id}. Reassign the remaining collection."),
             "schedule_id": schedule_id})
        db.commit()
        return {"success": True}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@app.put("/api/collections/{record_id}/reassign")
async def reassign_collection(record_id: str, request: dict,
                              current_user: dict = Depends(get_current_user_record)):
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    db = SessionLocal()
    try:
        driver_id = request.get("driver_id")
        driver = db.execute(text("""SELECT id FROM users WHERE id=:id AND role='driver'
            AND (is_active IS NULL OR is_active=TRUE)"""), {"id": driver_id}).first()
        if not driver:
            raise HTTPException(status_code=400, detail="Choose an active driver")
        result = db.execute(text("""UPDATE collection_records
            SET driver_id=:driver, status='pending'
            WHERE id=:id AND status='paused' AND date=:today AND driver_id<>:driver"""),
            {"id": record_id, "driver": driver_id, "today": get_ph_time().strftime("%Y-%m-%d")})
        if result.rowcount != 1:
            raise HTTPException(status_code=409, detail="Assignment changed; refresh first")
        db.commit()
        return {"success": True}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


@app.put("/api/collections/{record_id}/collected")
async def mark_collected(
    record_id: str, request: dict, current_user: dict = Depends(get_current_user_record)
):
    db = SessionLocal()
    try:
        waste_amount_kg = request.get("wasteAmountKg", 0)
        biodegradable_kg = request.get("biodegradableKg", 0)
        non_biodegradable_kg = request.get("nonBiodegradableKg", 0)
        recyclable_kg = request.get("recyclableKg", 0)
        special_waste_kg = request.get("specialWasteKg", 0)
        notes = request.get("notes", "")
        driver_id = current_user.get("id")

        schedule_id = request.get("schedule_id") or record_id
        _, assignment = _owned_collection_schedule(db, schedule_id, current_user)
        _validate_collection_id(record_id, schedule_id, assignment)
        if assignment and assignment["status"] == "paused":
            raise HTTPException(status_code=409, detail="Collection paused: request admin reassignment")
        barangay = "Unknown"
        waste_type = "Unknown"
        time_slot = ""

        # Get barangay, waste_type, and time_slot from schedule
        try:
            sched = db.execute(
                text(
                    "SELECT barangay, collection_type, start_time, end_time FROM schedules WHERE id = :id"
                ),
                {"id": schedule_id},
            ).fetchone()
            if sched:
                barangay = sched[0] or "Unknown"
                waste_type = sched[1] or "Unknown"
                time_slot = f"{sched[2] or ''} - {sched[3] or ''}"
        except:
            pass

        today = get_ph_time().strftime("%Y-%m-%d")

        collection_values = {
            "id": record_id,
            "schedule_id": schedule_id,
            "driver_id": driver_id,
            "barangay": barangay,
            "waste_type": waste_type,
            "time_slot": time_slot,
            "date": today,
            "waste_amount": waste_amount_kg,
            "biodegradable": biodegradable_kg,
            "non_biodegradable": non_biodegradable_kg,
            "recyclable": recyclable_kg,
            "special_waste": special_waste_kg,
            "notes": notes,
        }
        if _is_sqlite(db):
            db.execute(
                text("""INSERT INTO collection_records
                       (id, schedule_id, driver_id, barangay, waste_type, time_slot, date, status, waste_amount_kg,
                        biodegradable_kg, non_biodegradable_kg, recyclable_kg,
                        special_waste_kg, notes, collected_at)
                       VALUES (:id, :schedule_id, :driver_id, :barangay, :waste_type, :time_slot, :date, 'completed', :waste_amount,
                        :biodegradable, :non_biodegradable, :recyclable,
                        :special_waste, :notes, CURRENT_TIMESTAMP)
                       ON CONFLICT(id) DO UPDATE SET
                        status = 'completed',
                        waste_amount_kg = excluded.waste_amount_kg,
                        biodegradable_kg = excluded.biodegradable_kg,
                        non_biodegradable_kg = excluded.non_biodegradable_kg,
                        recyclable_kg = excluded.recyclable_kg,
                        special_waste_kg = excluded.special_waste_kg,
                        notes = excluded.notes,
                        collected_at = CURRENT_TIMESTAMP"""),
                collection_values,
            )
        else:
            db.execute(
                text("""INSERT INTO collection_records
                       (id, schedule_id, driver_id, barangay, waste_type, time_slot, date, status, waste_amount_kg,
                        biodegradable_kg, non_biodegradable_kg, recyclable_kg,
                        special_waste_kg, notes, collected_at)
                       VALUES (:id, :schedule_id, :driver_id, :barangay, :waste_type, :time_slot, :date, 'completed', :waste_amount,
                        :biodegradable, :non_biodegradable, :recyclable,
                        :special_waste, :notes, CURRENT_TIMESTAMP)
                       ON DUPLICATE KEY UPDATE
                        status = 'completed',
                        waste_amount_kg = VALUES(waste_amount_kg),
                        biodegradable_kg = VALUES(biodegradable_kg),
                        non_biodegradable_kg = VALUES(non_biodegradable_kg),
                        recyclable_kg = VALUES(recyclable_kg),
                        special_waste_kg = VALUES(special_waste_kg),
                        notes = VALUES(notes),
                        collected_at = CURRENT_TIMESTAMP"""),
                collection_values,
            )
        db.commit()

        # Completing one assignment must not end the driver's other routes.
        return {"success": True, "message": "Collection recorded successfully"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.put("/api/collections/{record_id}/skip")
async def skip_collection(
    record_id: str, request: dict, current_user: dict = Depends(get_current_user_record)
):
    db = SessionLocal()
    try:
        reason = request.get("reason", "Skipped by driver")
        driver_id = current_user.get("id")

        schedule_id = request.get("schedule_id") or record_id
        _, assignment = _owned_collection_schedule(db, schedule_id, current_user)
        _validate_collection_id(record_id, schedule_id, assignment)
        if assignment and assignment["status"] == "paused":
            raise HTTPException(status_code=409, detail="Collection paused: request admin reassignment")
        barangay = "Unknown"
        waste_type = "Unknown"
        time_slot = ""
        try:
            sched = db.execute(
                text(
                    "SELECT barangay, collection_type, start_time, end_time FROM schedules WHERE id = :id"
                ),
                {"id": schedule_id},
            ).fetchone()
            if sched:
                barangay = sched[0] or "Unknown"
                waste_type = sched[1] or "Unknown"
                time_slot = f"{sched[2] or ''} - {sched[3] or ''}"
        except Exception:
            pass

        # `date` must be set or start-route (which filters on DATE(date)) never
        # sees the skip and the stop comes back as pending.
        skip_params = {
            "id": record_id,
            "schedule_id": schedule_id,
            "driver_id": driver_id,
            "barangay": barangay,
            "waste_type": waste_type,
            "time_slot": time_slot,
            "date": get_ph_time().strftime("%Y-%m-%d"),
            "reason": reason,
        }
        if db.get_bind().dialect.name == "sqlite":
            db.execute(
                text("""INSERT INTO collection_records
                       (id, schedule_id, driver_id, barangay, waste_type, time_slot, date,
                        status, skip_reason, skipped_at)
                       VALUES (:id, :schedule_id, :driver_id, :barangay, :waste_type, :time_slot, :date,
                        'skipped', :reason, CURRENT_TIMESTAMP)
                       ON CONFLICT(id) DO UPDATE SET
                        status = 'skipped',
                        skip_reason = excluded.skip_reason,
                        skipped_at = CURRENT_TIMESTAMP"""),
                skip_params,
            )
        else:
            db.execute(
                text("""INSERT INTO collection_records
                       (id, schedule_id, driver_id, barangay, waste_type, time_slot, date,
                        status, skip_reason, skipped_at)
                       VALUES (:id, :schedule_id, :driver_id, :barangay, :waste_type, :time_slot, :date,
                        'skipped', :reason, CURRENT_TIMESTAMP)
                       ON DUPLICATE KEY UPDATE
                        status = 'skipped',
                        skip_reason = VALUES(skip_reason),
                        skipped_at = CURRENT_TIMESTAMP"""),
                skip_params,
            )
        db.commit()

        # Skipping one assignment must not end the driver's other routes.
        return {"success": True, "message": "Collection skipped"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.get("/api/collections/daily")
async def get_daily_collections(driverId: str = None, limit: int = 100, offset: int = 0, current_user: dict = Depends(get_current_user_record)):
    if current_user.get("role") not in ("driver", "admin"):
        raise HTTPException(status_code=403, detail="Driver access required")
    if current_user.get("role") == "driver":
        driverId = current_user.get("id")
    db = SessionLocal()
    try:
        limit, offset = _normalize_pagination(limit, offset, default_limit=100, max_limit=200)
        # get_ph_weekday() returns 0=Monday...6=Sunday (Philippine Time)
        # Database stores 0=Monday...6=Sunday, so direct mapping works
        today = get_ph_weekday()
        query = """SELECT s.*, u.full_name as driver_name
                   FROM schedules s
                   LEFT JOIN users u ON s.driver_id = u.id
                   WHERE s.day_of_week = :today AND s.is_active = 1"""

        params = {"today": today}
        if driverId:
            query += """ AND COALESCE((SELECT cr.driver_id FROM collection_records cr
                WHERE cr.schedule_id=s.id AND cr.date=:assignment_date LIMIT 1), s.driver_id)=:assignment_driver"""
            params["assignment_date"] = get_ph_time().strftime("%Y-%m-%d")
            params["assignment_driver"] = driverId
        # Always order by start_time
        query += " ORDER BY s.start_time LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        result = db.execute(text(query), params).fetchall()

        # Get all existing collection records for TODAY only
        today_date = get_ph_time().strftime("%Y-%m-%d")
        existing_records = {}
        try:
            existing = db.execute(
                text(
                    "SELECT schedule_id, id, status, waste_amount_kg, driver_id FROM collection_records WHERE DATE(date) = :today_date"
                ),
                {"today_date": today_date},
            ).fetchall()
            for rec in existing:
                sid = str(rec[0]) if rec[0] else None
                if sid:
                    existing_records[sid] = {
                        "id": str(rec[1]) if rec[1] else None,
                        "status": str(rec[2]) if rec[2] else "pending",
                        "waste_amount_kg": float(rec[3]) if rec[3] else 0,
                        "driver_id": rec[4],
                    }
            pass
        except Exception as e:
            logging.warning("Error fetching collection records: %s", e)

        records = []
        driver_names = dict(db.execute(text("SELECT id, full_name FROM users WHERE role='driver'")).fetchall())
        for row in result:
            mapping = row._mapping
            schedule_id = mapping.get("id")

            # Check if there's a collection record for this schedule
            assigned_driver = existing_records.get(str(schedule_id), {}).get("driver_id") or mapping.get("driver_id")
            if driverId and assigned_driver != driverId:
                continue
            status = "pending"
            existing_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"etrak:{schedule_id}:{today_date}"))
            waste_amount_kg = 0.0

            if schedule_id and str(schedule_id) in existing_records:
                existing_id = existing_records[str(schedule_id)]["id"] or existing_id
                status = existing_records[str(schedule_id)]["status"] or "pending"
                waste_amount_kg = (
                    existing_records[str(schedule_id)]["waste_amount_kg"] or 0.0
                )

            records.append(
                {
                    "id": existing_id,
                    **route_payload(mapping),
                    "schedule_id": schedule_id,
                    "barangay": mapping.get("barangay"),
                    "waste_type": mapping.get("collection_type", ""),
                    "time_slot": f"{mapping.get('start_time', '')} - {mapping.get('end_time', '')}",
                    "status": status,
                    "driver_id": assigned_driver,
                    "driver_name": driver_names.get(assigned_driver, mapping.get("driver_name")),
                    "waste_amount_kg": waste_amount_kg,
                }
            )
        return {"success": True, "data": records}
    finally:
        db.close()


# ── Admin Database Management ──────────────────────────────────────────

ADMIN_DB_TABLES = [
    "users", "schedules", "collection_records", "truck_locations",
    "rewards", "redemptions", "surveys", "survey_responses",
    "notifications", "reports", "barangays", "announcements",
    "app_update", "scan_activities", "devices",
]


@app.get("/api/admin/db/tables")
async def db_list_tables(current_user: dict = Depends(get_current_user_record)):
    """List all tables with row counts."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    db = SessionLocal()
    try:
        tables = []
        for name in ADMIN_DB_TABLES:
            try:
                count = db.execute(text(f"SELECT COUNT(*) FROM {name}")).fetchone()[0]
            except Exception:
                count = -1
            tables.append({"name": name, "row_count": count})
        return {"success": True, "data": tables}
    finally:
        db.close()


@app.get("/api/admin/db/table/{table_name}")
async def db_list_rows(
    table_name: str,
    page: int = 1,
    per_page: int = 50,
    search: str = "",
    sort_by: str = "",
    sort_dir: str = "ASC",
    current_user: dict = Depends(get_current_user_record),
):
    """List rows from a table with pagination, search, and sorting."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    db = SessionLocal()
    try:
        total = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]

        where_clause = ""
        params: dict = {}
        if search:
            cols = db.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
            text_cols = [c[1] for c in cols if "VARCHAR" in (c[2] or "").upper() or "TEXT" in (c[2] or "").upper() or c[2] is None]
            if text_cols:
                likes = " OR ".join([f"{c} LIKE :search" for c in text_cols])
                where_clause = f"WHERE {likes}"
                params["search"] = f"%{search}%"

        order = ""
        if sort_by:
            direction = "DESC" if sort_dir.upper() == "DESC" else "ASC"
            order = f"ORDER BY {sort_by} {direction}"

        limit = per_page
        offset = (page - 1) * per_page
        sql = f"SELECT * FROM {table_name} {where_clause} {order} LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        rows = db.execute(text(sql), params).fetchall()
        cols = db.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        col_names = [c[1] for c in cols]

        data = []
        for row in rows:
            data.append({col_names[i]: row[i] for i in range(len(col_names))})

        return {
            "success": True,
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "columns": col_names,
        }
    finally:
        db.close()


@app.get("/api/admin/db/row/{table_name}/{row_id}")
async def db_get_row(
    table_name: str,
    row_id: str,
    current_user: dict = Depends(get_current_user_record),
):
    """Get a single row by ID."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    db = SessionLocal()
    try:
        cols = db.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        col_names = [c[1] for c in cols]
        row = db.execute(text(f"SELECT * FROM {table_name} WHERE id = :id"), {"id": row_id}).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Row not found")
        return {"success": True, "data": {col_names[i]: row[i] for i in range(len(col_names))}}
    finally:
        db.close()


@app.put("/api/admin/db/row/{table_name}/{row_id}")
async def db_update_row(
    table_name: str,
    row_id: str,
    request: dict,
    current_user: dict = Depends(get_current_user_record),
):
    """Update a row by ID. Pass fields to update in the request body."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    updates = request.get("data", {})
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    db = SessionLocal()
    try:
        cols = db.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        valid_cols = {c[1] for c in cols}
        set_parts = []
        params: dict = {"id": row_id}
        for k, v in updates.items():
            if k in valid_cols and k != "id":
                set_parts.append(f"{k} = :{k}")
                params[k] = v
        if not set_parts:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        sql = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE id = :id"
        db.execute(text(sql), params)
        db.commit()
        return {"success": True, "message": f"Updated {len(set_parts)} field(s)"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/admin/db/row/{table_name}/{row_id}")
async def db_delete_row(
    table_name: str,
    row_id: str,
    current_user: dict = Depends(get_current_user_record),
):
    """Delete a row by ID."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    db = SessionLocal()
    try:
        result = db.execute(text(f"DELETE FROM {table_name} WHERE id = :id"), {"id": row_id})
        db.commit()
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
        return {"success": True, "message": "Row deleted"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.delete("/api/admin/db/rows/{table_name}")
async def db_delete_rows(
    table_name: str,
    request: dict,
    current_user: dict = Depends(get_current_user_record),
):
    """Delete multiple rows by IDs."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    ids = request.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="No IDs provided")

    db = SessionLocal()
    try:
        placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
        params = {f"id_{i}": rid for i, rid in enumerate(ids)}
        result = db.execute(text(f"DELETE FROM {table_name} WHERE id IN ({placeholders})"), params)
        db.commit()
        return {"success": True, "message": f"Deleted {result.rowcount} row(s)"}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/admin/db/row/{table_name}")
async def db_insert_row(
    table_name: str,
    request: dict,
    current_user: dict = Depends(get_current_user_record),
):
    """Insert a new row into a table."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if table_name not in ADMIN_DB_TABLES:
        raise HTTPException(status_code=400, detail=f"Unknown table: {table_name}")

    data = request.get("data", {})
    if not data:
        raise HTTPException(status_code=400, detail="No data provided")

    import uuid
    if "id" not in data:
        data["id"] = str(uuid.uuid4())

    db = SessionLocal()
    try:
        cols = db.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        valid_cols = {c[1] for c in cols}
        filtered = {k: v for k, v in data.items() if k in valid_cols}
        if not filtered:
            raise HTTPException(status_code=400, detail="No valid columns in data")

        col_str = ", ".join(filtered.keys())
        val_str = ", ".join([f":{k}" for k in filtered.keys()])
        db.execute(text(f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str})"), filtered)
        db.commit()
        return {"success": True, "message": "Row inserted", "id": data.get("id")}
    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@app.post("/api/admin/db/raw")
async def db_raw_query(
    request: dict,
    current_user: dict = Depends(get_current_user_record),
):
    """Execute a raw SQL query (SELECT only for safety)."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    sql = request.get("sql", "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="No SQL provided")

    upper = sql.upper().lstrip()
    if not upper.startswith("SELECT") and not upper.startswith("PRAGMA"):
        raise HTTPException(status_code=400, detail="Only SELECT and PRAGMA queries allowed")

    db = SessionLocal()
    try:
        result = db.execute(text(sql))
        rows = result.fetchall()
        cols = result.keys() if hasattr(result, "keys") else []
        data = [dict(zip(cols, row)) for row in rows]
        return {"success": True, "data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()
import sys
import os
sys.path.append(os.path.dirname(__file__))

try:
    from methodchannel.methodchannel import handle_method_call
    METHOD_CHANNEL_AVAILABLE = True
except ImportError:
    METHOD_CHANNEL_AVAILABLE = False
    print("Warning: MethodChannel handlers not available")


@app.post("/methodchannel")
async def method_channel_endpoint(
    request: dict,
    current_user: dict = Depends(get_current_user_record),
):
    """Handle MethodChannel calls over HTTP (admin only).

    The Flutter UI uses the native platform channel, not this route. Over the
    Cloudflare tunnel this would otherwise let anyone stop the server or
    reconfigure the tunnel, so it requires an admin token.
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    if not METHOD_CHANNEL_AVAILABLE:
        return {"status": "error", "message": "MethodChannel not available"}
    
    method_name = request.get("method")
    args = request.get("args", {})
    
    if not method_name:
        return {"status": "error", "message": "Method name not provided"}
    
    try:
        result = handle_method_call(method_name, args)
        return {"status": "success", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
