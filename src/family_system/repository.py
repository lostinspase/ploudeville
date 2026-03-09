from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import secrets
from datetime import date, datetime, timedelta
from typing import Iterable
from urllib.parse import urlencode
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .db import get_connection

WEEKLY_REQUIRED_MINIMUM = 3
CARE_TYPES = ("feed", "water", "nurture")
WEEKLY_ALLOWANCE_DEFAULT_SCOPE = "weekly_allowance_default"
WEEKLY_ALLOWANCE_OVERRIDE_SCOPE = "weekly_allowance_override"
WEEKLY_PERIOD_DAY_OF_WEEK = "day_of_week"
WEEKLY_PERIOD_ALL_DAYS = "all_days"
WEEKLY_PERIOD_TIMES_PER_PERIOD = "times_per_period"


def _pin_hash(pin: str) -> str:
    return hashlib.sha256(pin.encode("utf-8")).hexdigest()


def _password_hash(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _as_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _as_hhmm(value: str) -> str:
    cleaned = value.strip()
    try:
        datetime.strptime(cleaned, "%H:%M")
    except ValueError as err:
        raise ValueError("Time must be in HH:MM format") from err
    return cleaned


def _parse_birthdate(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def current_week_key(on_date: date | None = None) -> str:
    d = on_date or date.today()
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _normalize_week_key(week_key: str) -> str:
    start, _ = _week_bounds(week_key.strip())
    return current_week_key(start)


def _week_bounds(week_key: str) -> tuple[date, date]:
    try:
        year_text, week_text = week_key.split("-W", 1)
        iso_year = int(year_text)
        iso_week = int(week_text)
    except ValueError as err:
        raise ValueError("Invalid week key format. Expected YYYY-Www") from err
    start = date.fromisocalendar(iso_year, iso_week, 1)
    end = start + timedelta(days=6)
    return start, end


def _normalize_weekly_period_mode(period_mode: str | None) -> str:
    cleaned = (period_mode or WEEKLY_PERIOD_DAY_OF_WEEK).strip().lower()
    if cleaned not in (
        WEEKLY_PERIOD_DAY_OF_WEEK,
        WEEKLY_PERIOD_ALL_DAYS,
        WEEKLY_PERIOD_TIMES_PER_PERIOD,
    ):
        raise ValueError("Invalid weekly period mode")
    return cleaned


def _normalize_times_per_period(times_per_period: int | None) -> int:
    value = int(times_per_period or 1)
    if value < 1 or value > 7:
        raise ValueError("times_per_period must be between 1 and 7")
    return value


def add_child(
    name: str,
    age: int,
    birthdate: str | None = None,
    email: str | None = None,
    text_number: str | None = None,
) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO children (name, age, birthdate, email, text_number) VALUES (?, ?, ?, ?, ?)",
            (
                name.strip(),
                age,
                birthdate.strip() if birthdate else None,
                email.strip() if email else None,
                text_number.strip() if text_number else None,
            ),
        )
        return int(cursor.lastrowid)


def add_or_update_child(
    name: str,
    age: int,
    birthdate: str | None = None,
    email: str | None = None,
    text_number: str | None = None,
) -> int:
    cleaned_name = name.strip()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO children (name, age, birthdate, email, text_number, active)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(name) DO UPDATE SET
                age = excluded.age,
                birthdate = COALESCE(excluded.birthdate, children.birthdate),
                email = COALESCE(excluded.email, children.email),
                text_number = COALESCE(excluded.text_number, children.text_number),
                active = 1
            """,
            (
                cleaned_name,
                age,
                birthdate.strip() if birthdate else None,
                email.strip() if email else None,
                text_number.strip() if text_number else None,
            ),
        )
        row = conn.execute(
            "SELECT id FROM children WHERE name = ?",
            (cleaned_name,),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to fetch child id after upsert")
        child_id = int(row["id"])

        default_rules: dict[str, tuple[str, str, str, str]] = {
            "Elliana": ("20:15", "21:00", "21:00", "21:30"),
            "Gracelyn": ("20:15", "21:00", "21:00", "21:30"),
            "Gracie": ("20:15", "21:00", "21:00", "21:30"),
            "Rosie": ("19:45", "20:00", "20:15", "20:30"),
        }
        rules = default_rules.get(cleaned_name)
        if rules:
            conn.execute(
                """
                INSERT OR IGNORE INTO child_house_rules (
                    child_id, weekday_screen_off, weekday_bedtime, weekend_screen_off, weekend_bedtime
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (child_id, rules[0], rules[1], rules[2], rules[3]),
            )

        app_defaults: dict[str, int] = {
            "Elliana": 60,
            "Gracelyn": 60,
            "Gracie": 60,
            "Rosie": 0,
        }
        app_minutes = app_defaults.get(cleaned_name)
        if app_minutes is not None:
            conn.execute(
                """
                INSERT OR IGNORE INTO child_app_limits (child_id, app_name, minutes_per_day, active)
                VALUES (?, 'Roblox', ?, 1)
                """,
                (child_id, app_minutes),
            )
        return child_id


def list_children(active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            c.id,
            c.name,
            c.age,
            c.birthdate,
            c.email,
            c.text_number,
            (c.pin_hash IS NOT NULL) AS has_pin,
            c.default_pet_species_id,
            c.default_pet_name,
            ps.name AS default_pet_species_name,
            c.active,
            c.created_at
        FROM children c
        LEFT JOIN pet_species ps ON ps.id = c.default_pet_species_id
    """
    params: tuple = ()
    if active_only:
        query += " WHERE c.active = 1"
    query += " ORDER BY c.name"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def get_child(child_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                c.id,
                c.name,
                c.age,
                c.birthdate,
                c.email,
                c.text_number,
                c.pin_hash,
                c.default_pet_species_id,
                c.default_pet_name,
                ps.name AS default_pet_species_name,
                c.active,
                c.created_at
            FROM children c
            LEFT JOIN pet_species ps ON ps.id = c.default_pet_species_id
            WHERE c.id = ?
            """,
            (child_id,),
        ).fetchone()
        return dict(row) if row else None


def set_child_pin(child_id: int, pin: str) -> bool:
    normalized = pin.strip()
    if not normalized.isdigit() or not 4 <= len(normalized) <= 8:
        raise ValueError("PIN must be 4-8 digits")
    with get_connection() as conn:
        cursor = conn.execute(
            "UPDATE children SET pin_hash = ? WHERE id = ?",
            (_pin_hash(normalized), child_id),
        )
        return cursor.rowcount > 0


def verify_child_pin(child_id: int, pin: str) -> bool:
    child = get_child(child_id)
    if not child:
        return False
    stored_hash = child.get("pin_hash")
    if not stored_hash:
        return False
    return _pin_hash(pin.strip()) == stored_hash


def update_child_contact_info(child_id: int, email: str = "", text_number: str = "") -> bool:
    cleaned_email = email.strip()
    cleaned_text = text_number.strip()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE children
            SET email = ?, text_number = ?
            WHERE id = ?
            """,
            (cleaned_email or None, cleaned_text or None, child_id),
        )
        return cursor.rowcount > 0


def list_pet_species(active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            ps.id,
            ps.name,
            ps.rarity,
            ps.is_custom,
            ps.created_by_child_id,
            c.name AS created_by_child_name,
            ps.active
        FROM pet_species ps
        LEFT JOIN children c ON c.id = ps.created_by_child_id
    """
    if active_only:
        query += " WHERE ps.active = 1"
    query += " ORDER BY ps.name ASC"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def create_pet_species(name: str, rarity: str = "custom", created_by_child_id: int | None = None) -> int:
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Pet species name is required")
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO pet_species (name, rarity, is_custom, created_by_child_id, active)
                VALUES (?, ?, 1, ?, 1)
                """,
                (cleaned, rarity.strip() or "custom", created_by_child_id),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError as err:
            raise ValueError(f"Pet species already exists: {cleaned}") from err


def set_default_pet(child_id: int, pet_species_id: int, pet_name: str) -> bool:
    if not pet_name.strip():
        raise ValueError("Pet name is required")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE children
            SET default_pet_species_id = ?, default_pet_name = ?
            WHERE id = ?
            """,
            (pet_species_id, pet_name.strip(), child_id),
        )
        return cursor.rowcount > 0


def weekly_required_completed_count(child_id: int, week_key: str) -> int:
    start, end = _week_bounds(week_key)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(1) AS cnt
            FROM task_completions tc
            JOIN tasks t ON t.id = tc.task_id
            WHERE tc.child_id = ?
              AND tc.status = 'approved'
              AND t.rank = 'required'
              AND date(datetime(tc.completed_at, 'localtime')) BETWEEN date(?) AND date(?)
            """,
            (child_id, start.isoformat(), end.isoformat()),
        ).fetchone()
        return int(row["cnt"]) if row else 0


def child_pet_unlocked(child_id: int, minimum_required: int = WEEKLY_REQUIRED_MINIMUM) -> bool:
    with get_connection() as conn:
        has_adoption = conn.execute(
            "SELECT 1 FROM pet_adoptions WHERE child_id = ? LIMIT 1",
            (child_id,),
        ).fetchone()
        if has_adoption:
            return True

    today = date.today()
    # Check current and previous 12 weeks as practical history window.
    for offset in range(0, 13):
        wk = current_week_key(today - timedelta(days=7 * offset))
        if weekly_required_completed_count(child_id, wk) >= minimum_required:
            return True
    return False


def adopt_weekly_pet(
    child_id: int,
    pet_species_id: int,
    pet_name: str,
    week_key: str | None = None,
    minimum_required: int = WEEKLY_REQUIRED_MINIMUM,
) -> int:
    wk = week_key or current_week_key()
    if not pet_name.strip():
        raise ValueError("Pet name is required")
    if not child_pet_unlocked(child_id, minimum_required=minimum_required):
        raise ValueError(
            f"Not unlocked yet. Complete at least {minimum_required} required tasks in a week first."
        )
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO pet_adoptions (child_id, week_key, pet_species_id, pet_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(child_id, week_key) DO UPDATE SET
                pet_species_id = excluded.pet_species_id,
                pet_name = excluded.pet_name
            """,
            (child_id, wk, pet_species_id, pet_name.strip()),
        )
        row = conn.execute(
            "SELECT id FROM pet_adoptions WHERE child_id = ? AND week_key = ?",
            (child_id, wk),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to load adoption record")
        # If no default pet yet, set this one as default.
        child = conn.execute(
            "SELECT default_pet_species_id FROM children WHERE id = ?",
            (child_id,),
        ).fetchone()
        if child and child["default_pet_species_id"] is None:
            conn.execute(
                """
                UPDATE children
                SET default_pet_species_id = ?, default_pet_name = ?
                WHERE id = ?
                """,
                (pet_species_id, pet_name.strip(), child_id),
            )
        return int(row["id"])


def get_current_pet(child_id: int, week_key: str | None = None) -> dict | None:
    wk = week_key or current_week_key()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                pa.id,
                pa.week_key,
                pa.pet_name,
                ps.id AS pet_species_id,
                ps.name AS pet_species_name
            FROM pet_adoptions pa
            JOIN pet_species ps ON ps.id = pa.pet_species_id
            WHERE pa.child_id = ? AND pa.week_key = ?
            """,
            (child_id, wk),
        ).fetchone()
        if row:
            data = dict(row)
            data["source"] = "weekly"
            return data
        fallback = conn.execute(
            """
            SELECT
                c.default_pet_name AS pet_name,
                ps.id AS pet_species_id,
                ps.name AS pet_species_name
            FROM children c
            LEFT JOIN pet_species ps ON ps.id = c.default_pet_species_id
            WHERE c.id = ?
            """,
            (child_id,),
        ).fetchone()
        if not fallback or fallback["pet_species_id"] is None:
            return None
        data = dict(fallback)
        data["week_key"] = wk
        data["source"] = "default"
        return data


def list_pet_adoptions(child_id: int | None = None, limit: int = 100) -> Iterable[dict]:
    query = """
        SELECT
            pa.id,
            pa.child_id,
            c.name AS child_name,
            pa.week_key,
            pa.pet_name,
            ps.name AS pet_species_name,
            pa.created_at
        FROM pet_adoptions pa
        JOIN children c ON c.id = pa.child_id
        JOIN pet_species ps ON ps.id = pa.pet_species_id
    """
    params: list[object] = []
    if child_id is not None:
        query += " WHERE pa.child_id = ?"
        params.append(child_id)
    query += " ORDER BY pa.week_key DESC, pa.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def list_pet_care_status(child_id: int, week_key: str | None = None) -> dict[str, bool]:
    wk = week_key or current_week_key()
    status = {care: False for care in CARE_TYPES}
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT care_type
            FROM pet_care
            WHERE child_id = ? AND week_key = ?
            """,
            (child_id, wk),
        ).fetchall()
        for row in rows:
            care = str(row["care_type"])
            if care in status:
                status[care] = True
    return status


def complete_pet_care(child_id: int, care_type: str, week_key: str | None = None) -> int:
    wk = week_key or current_week_key()
    if care_type not in CARE_TYPES:
        raise ValueError(f"care_type must be one of: {', '.join(CARE_TYPES)}")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO pet_care (child_id, week_key, care_type)
            VALUES (?, ?, ?)
            """,
            (child_id, wk, care_type),
        )
        row = conn.execute(
            """
            SELECT id
            FROM pet_care
            WHERE child_id = ? AND week_key = ? AND care_type = ?
            """,
            (child_id, wk, care_type),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to record pet care")
        return int(row["id"])


def pet_care_streak_weeks(child_id: int, max_weeks: int = 52) -> int:
    streak = 0
    today = date.today()
    for i in range(max_weeks):
        wk = current_week_key(today - timedelta(days=7 * i))
        care = list_pet_care_status(child_id, wk)
        if all(care.values()):
            streak += 1
        else:
            break
    return streak


def award_weekly_pet_badges(
    child_id: int,
    week_key: str | None = None,
    minimum_required: int = WEEKLY_REQUIRED_MINIMUM,
) -> int:
    wk = week_key or current_week_key()
    care = list_pet_care_status(child_id, wk)
    required_done = weekly_required_completed_count(child_id, wk)
    streak = pet_care_streak_weeks(child_id)
    badges: list[tuple[str, str, str]] = []

    if all(care.values()):
        badges.append(("care_champion", "Care Champion", "Completed feed, water, and nurture this week."))
    if required_done >= minimum_required:
        badges.append(("allowance_hero", "Allowance Hero", "Met the weekly required-task target."))
    if all(care.values()) and required_done >= minimum_required:
        badges.append(("pet_guardian", "Pet Guardian", "Kept your pet healthy and stayed on top of required tasks."))
    if streak >= 2:
        badges.append(("care_streak", "Care Streak", f"Maintained complete pet care for {streak} week(s) in a row."))

    created = 0
    with get_connection() as conn:
        for code, name, description in badges:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO pet_badges (
                    child_id, week_key, badge_code, badge_name, description
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (child_id, wk, code, name, description),
            )
            if cursor.rowcount > 0:
                created += 1
    return created


def list_pet_badges(child_id: int | None = None, limit: int = 100) -> Iterable[dict]:
    query = """
        SELECT
            pb.id,
            pb.child_id,
            c.name AS child_name,
            pb.week_key,
            pb.badge_code,
            pb.badge_name,
            pb.description,
            pb.created_at
        FROM pet_badges pb
        JOIN children c ON c.id = pb.child_id
    """
    params: list[object] = []
    if child_id is not None:
        query += " WHERE pb.child_id = ?"
        params.append(child_id)
    query += " ORDER BY pb.week_key DESC, pb.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def add_message(
    sender_type: str,
    sender_name: str,
    message_text: str,
    child_id: int | None = None,
    week_key: str | None = None,
    message_kind: str = "",
) -> int:
    if not message_text.strip():
        raise ValueError("Message text is required")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO messages (
                sender_type,
                sender_name,
                child_id,
                message_text,
                week_key,
                message_kind
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (sender_type, sender_name.strip(), child_id, message_text.strip(), week_key, message_kind.strip()),
        )
        row = conn.execute(
            """
            SELECT id
            FROM messages
            WHERE sender_type = ?
              AND sender_name = ?
              AND COALESCE(child_id, -1) = COALESCE(?, -1)
              AND message_text = ?
              AND COALESCE(week_key, '') = COALESCE(?, '')
              AND COALESCE(message_kind, '') = COALESCE(?, '')
            ORDER BY id DESC
            LIMIT 1
            """,
            (sender_type, sender_name.strip(), child_id, message_text.strip(), week_key, message_kind.strip()),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to store message")
        return int(row["id"])


def list_messages(child_id: int | None = None, limit: int = 100) -> Iterable[dict]:
    query = """
        SELECT
            m.id,
            m.sender_type,
            m.sender_name,
            m.child_id,
            c.name AS child_name,
            m.message_text,
            m.week_key,
            m.message_kind,
            m.created_at
        FROM messages m
        LEFT JOIN children c ON c.id = m.child_id
    """
    params: list[object] = []
    if child_id is not None:
        query += " WHERE (m.child_id = ? OR m.child_id IS NULL)"
        params.append(child_id)
    query += " ORDER BY m.created_at DESC, m.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def generate_pet_help_messages(child_id: int, week_key: str | None = None) -> int:
    wk = week_key or current_week_key()
    pet = get_current_pet(child_id, wk)
    if not pet:
        return 0
    child = get_child(child_id)
    if not child:
        return 0
    care_status = list_pet_care_status(child_id, wk)
    created = 0
    for care_type in CARE_TYPES:
        if care_status[care_type]:
            continue
        text_map = {
            "feed": f"Help me {child['name']}, I need food!!!",
            "water": f"Help me {child['name']}, I need water!!!",
            "nurture": f"Help me {child['name']}, I need love and nurture!!!",
        }
        kind = f"pet_help_{care_type}"
        add_message(
            sender_type="pet",
            sender_name=str(pet["pet_name"]),
            message_text=text_map[care_type],
            child_id=child_id,
            week_key=wk,
            message_kind=kind,
        )
        created += 1
    return created


def get_pet_weekly_dashboard(
    child_id: int,
    week_key: str | None = None,
    minimum_required: int = WEEKLY_REQUIRED_MINIMUM,
) -> dict:
    wk = week_key or current_week_key()
    pet = get_current_pet(child_id, wk)
    care = list_pet_care_status(child_id, wk)
    required_done = weekly_required_completed_count(child_id, wk)
    award_weekly_pet_badges(child_id, wk, minimum_required=minimum_required)
    badges = list_pet_badges(child_id=child_id, limit=20)
    streak = pet_care_streak_weeks(child_id)
    missing = [k for k, done in care.items() if not done]
    health = "healthy" if pet and not missing else "needs_help"
    return {
        "week_key": wk,
        "pet": pet,
        "care": care,
        "missing_care": missing,
        "required_completed": required_done,
        "required_minimum": minimum_required,
        "adoption_unlocked": child_pet_unlocked(child_id, minimum_required),
        "health": health,
        "care_streak_weeks": streak,
        "badges": badges,
    }


def get_setting(key: str) -> str | None:
    with get_connection() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else None


def set_setting(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def get_wallet_daily_interest_rate_percent() -> float:
    raw = get_setting("wallet_daily_interest_rate_percent") or "0.05"
    try:
        value = float(raw)
    except ValueError:
        value = 0.05
    return max(value, 0.0)


def set_wallet_daily_interest_rate_percent(rate_percent: float) -> None:
    value = round(float(rate_percent), 4)
    if value < 0:
        raise ValueError("Interest rate must be >= 0")
    set_setting("wallet_daily_interest_rate_percent", f"{value:.4f}".rstrip("0").rstrip("."))


def apply_daily_wallet_interest(on_date: str | date | None = None) -> int:
    d = _as_date(on_date or date.today())
    accrual_date = d.isoformat()
    rate_percent = get_wallet_daily_interest_rate_percent()
    if rate_percent <= 0:
        return 0
    created = 0
    with get_connection() as conn:
        children = conn.execute("SELECT id FROM children WHERE active = 1").fetchall()
        for child in children:
            child_id = int(child["id"])
            exists = conn.execute(
                """
                SELECT 1
                FROM interest_accruals
                WHERE child_id = ? AND accrual_date = ?
                """,
                (child_id, accrual_date),
            ).fetchone()
            if exists:
                continue
            balance_row = conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0) AS balance
                FROM ledger_entries
                WHERE child_id = ? AND asset_type = 'allowance'
                """,
                (child_id,),
            ).fetchone()
            opening_balance = float(balance_row["balance"]) if balance_row else 0.0
            if opening_balance <= 0:
                continue
            interest_amount = round(opening_balance * (rate_percent / 100.0), 2)
            if interest_amount <= 0:
                continue

            conn.execute(
                """
                INSERT INTO interest_accruals (
                    child_id, accrual_date, rate_percent, opening_balance, interest_amount
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (child_id, accrual_date, rate_percent, opening_balance, interest_amount),
            )
            conn.execute(
                """
                INSERT INTO ledger_entries (
                    child_id, asset_type, amount, source_type, note
                )
                VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
                """,
                (
                    child_id,
                    interest_amount,
                    f"Daily interest {rate_percent:.4f}% on ${opening_balance:.2f} ({accrual_date})",
                ),
            )
            created += 1
    return created


def verify_parent_password(password: str) -> bool:
    stored = get_setting("parent_password_hash")
    if not stored:
        return False
    return _password_hash(password.strip()) == stored


def set_parent_password(password: str) -> None:
    cleaned = password.strip()
    if len(cleaned) < 6:
        raise ValueError("Parent password must be at least 6 characters")
    set_setting("parent_password_hash", _password_hash(cleaned))


def get_parent_reset_email() -> str:
    return get_setting("parent_reset_email") or "jploude@gmail.com"


def list_parents(active_only: bool = True) -> Iterable[dict]:
    query = "SELECT id, name, email, text_number, birthdate, active, created_at FROM parents"
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY name ASC"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def get_parent_reset_emails() -> list[str]:
    parents = list_parents(active_only=True)
    emails = [str(p["email"]) for p in parents if p.get("email")]
    if emails:
        return emails
    return [get_parent_reset_email()]


def get_parent_reset_text_numbers() -> list[str]:
    parents = list_parents(active_only=True)
    numbers = [str(p["text_number"]).strip() for p in parents if p.get("text_number")]
    return [n for n in numbers if n]


def update_parent_contact_info(parent_id: int, email: str = "", text_number: str = "") -> bool:
    cleaned_email = email.strip()
    cleaned_text = text_number.strip()
    if not cleaned_email:
        raise ValueError("Email is required")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE parents
            SET email = ?, text_number = ?
            WHERE id = ?
            """,
            (cleaned_email, cleaned_text or None, parent_id),
        )
        return cursor.rowcount > 0


def create_parent_reset_token(valid_minutes: int = 30, email: str | None = None) -> tuple[str, str]:
    token = secrets.token_urlsafe(24)
    expires_at = (datetime.utcnow() + timedelta(minutes=valid_minutes)).isoformat(timespec="seconds")
    target_email = (email or get_parent_reset_email()).strip()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO parent_reset_tokens (token, email, expires_at, used)
            VALUES (?, ?, ?, 0)
            """,
            (token, target_email, expires_at),
        )
    return token, target_email


def create_child_pin_reset_token(child_id: int, valid_minutes: int = 30) -> tuple[str, str, str]:
    child = get_child(child_id)
    if not child:
        raise ValueError("Child not found")
    email = str(child.get("email") or "").strip()
    if not email:
        raise ValueError("Child email is not set")
    token = secrets.token_urlsafe(24)
    expires_at = (datetime.utcnow() + timedelta(minutes=valid_minutes)).isoformat(timespec="seconds")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO child_pin_reset_tokens (child_id, token, email, expires_at, used)
            VALUES (?, ?, ?, ?, 0)
            """,
            (child_id, token, email, expires_at),
        )
    return token, email, str(child["name"])


def consume_child_pin_reset_token(token: str, new_pin: str) -> bool:
    normalized = new_pin.strip()
    if not normalized.isdigit() or not 4 <= len(normalized) <= 8:
        raise ValueError("PIN must be 4-8 digits")
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, child_id, expires_at, used
            FROM child_pin_reset_tokens
            WHERE token = ?
            """,
            (token.strip(),),
        ).fetchone()
        if not row:
            return False
        if int(row["used"]) == 1:
            return False
        expires_at = datetime.fromisoformat(str(row["expires_at"]))
        if datetime.utcnow() > expires_at:
            return False
        conn.execute(
            "UPDATE children SET pin_hash = ? WHERE id = ?",
            (_pin_hash(normalized), row["child_id"]),
        )
        conn.execute("UPDATE child_pin_reset_tokens SET used = 1 WHERE id = ?", (row["id"],))
    return True


def consume_parent_reset_token(token: str, new_password: str) -> bool:
    cleaned = new_password.strip()
    if len(cleaned) < 6:
        raise ValueError("Parent password must be at least 6 characters")
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, expires_at, used
            FROM parent_reset_tokens
            WHERE token = ?
            """,
            (token.strip(),),
        ).fetchone()
        if not row:
            return False
        if int(row["used"]) == 1:
            return False
        expires_at = datetime.fromisoformat(str(row["expires_at"]))
        if datetime.utcnow() > expires_at:
            return False
        conn.execute(
            "UPDATE app_settings SET value = ? WHERE key = 'parent_password_hash'",
            (_password_hash(cleaned),),
        )
        conn.execute("UPDATE parent_reset_tokens SET used = 1 WHERE id = ?", (row["id"],))
    return True


def count_pet_adoptions(child_id: int | None = None) -> int:
    query = "SELECT COUNT(1) AS cnt FROM pet_adoptions"
    params: tuple[object, ...] = ()
    if child_id is not None:
        query += " WHERE child_id = ?"
        params = (child_id,)
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
        return int(row["cnt"]) if row else 0


def is_child_birthday(child_id: int, on_date: date | None = None) -> bool:
    d = on_date or date.today()
    child = get_child(child_id)
    if not child:
        return False
    born = _parse_birthdate(child.get("birthdate"))
    if not born:
        return False
    return born.month == d.month and born.day == d.day


def list_today_birthdays(on_date: date | None = None) -> Iterable[dict]:
    d = on_date or date.today()
    children = list_children()
    return [c for c in children if _parse_birthdate(c.get("birthdate")) and is_child_birthday(int(c["id"]), d)]


def apply_birthday_treatment(child_id: int, on_date: date | None = None) -> bool:
    d = on_date or date.today()
    child = get_child(child_id)
    if not child:
        return False
    born = _parse_birthdate(child.get("birthdate"))
    if not born:
        return False
    if born.month != d.month or born.day != d.day:
        return False

    age_now = d.year - born.year
    allowance_bonus = float(max(age_now, 1))
    screen_time_bonus = float(max(age_now * 10, 10))

    with get_connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM birthday_events WHERE child_id = ? AND year = ?",
            (child_id, d.year),
        ).fetchone()
        if existing:
            return False

        conn.execute(
            """
            INSERT INTO birthday_events (child_id, year, allowance_bonus, screen_time_bonus)
            VALUES (?, ?, ?, ?)
            """,
            (child_id, d.year, allowance_bonus, screen_time_bonus),
        )
        conn.execute(
            """
            INSERT INTO ledger_entries (child_id, asset_type, amount, source_type, note)
            VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
            """,
            (child_id, allowance_bonus, f"Birthday bonus {d.year}"),
        )
        conn.execute(
            """
            INSERT INTO ledger_entries (child_id, asset_type, amount, source_type, note)
            VALUES (?, 'screen_time', ?, 'manual_adjustment', ?)
            """,
            (child_id, screen_time_bonus, f"Birthday bonus {d.year}"),
        )

    add_message(
        sender_type="system",
        sender_name="Birthday Bot",
        child_id=child_id,
        week_key=str(d.year),
        message_kind="birthday_bonus",
        message_text=(
            f"Happy Birthday {child['name']}! Special day treatment unlocked: "
            f"${allowance_bonus:.2f} allowance and {screen_time_bonus:.0f} minutes screen time."
        ),
    )
    add_message(
        sender_type="pet",
        sender_name=str((get_current_pet(child_id) or {}).get("pet_name") or "Your Pet"),
        child_id=child_id,
        week_key=str(d.year),
        message_kind="birthday_pet",
        message_text=f"Happy birthday {child['name']}! Let's celebrate together!",
    )
    return True


def list_today_parent_birthdays(on_date: date | None = None) -> Iterable[dict]:
    d = on_date or date.today()
    parents = list_parents(active_only=True)
    result = []
    for p in parents:
        born = _parse_birthdate(p.get("birthdate"))
        if born and born.month == d.month and born.day == d.day:
            result.append(p)
    return result


def apply_parent_birthday_treatment(parent_id: int, on_date: date | None = None) -> bool:
    d = on_date or date.today()
    with get_connection() as conn:
        parent = conn.execute(
            "SELECT id, name, birthdate FROM parents WHERE id = ? AND active = 1",
            (parent_id,),
        ).fetchone()
        if not parent:
            return False
        born = _parse_birthdate(parent["birthdate"])
        if not born or born.month != d.month or born.day != d.day:
            return False

        existing = conn.execute(
            "SELECT 1 FROM parent_birthday_events WHERE parent_id = ? AND year = ?",
            (parent_id, d.year),
        ).fetchone()
        if existing:
            return False
        conn.execute(
            "INSERT INTO parent_birthday_events (parent_id, year) VALUES (?, ?)",
            (parent_id, d.year),
        )

    add_message(
        sender_type="system",
        sender_name="Birthday Bot",
        child_id=None,
        week_key=str(d.year),
        message_kind=f"parent_birthday_{parent_id}",
        message_text=f"Happy Birthday {parent['name']}! Special treat day is ON!",
    )
    return True


def _normalize_website(url: str) -> str:
    cleaned = url.strip()
    if not cleaned:
        raise ValueError("Website is required")
    parsed = urlparse(cleaned)
    if not parsed.scheme:
        cleaned = f"https://{cleaned}"
        parsed = urlparse(cleaned)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Website must be a valid http(s) URL")
    return cleaned


def check_website_live(website: str, timeout_seconds: int = 5) -> bool:
    normalized = _normalize_website(website)
    headers = {"User-Agent": "PloudevilleFamilyTaskSystem/1.0"}
    for method in ("HEAD", "GET"):
        try:
            request = Request(normalized, headers=headers, method=method)
            with urlopen(request, timeout=timeout_seconds) as response:
                code = int(getattr(response, "status", 0) or 0)
                if 200 <= code < 400:
                    return True
        except Exception:  # noqa: BLE001
            continue
    return False


def add_charity(
    name: str,
    website: str,
    created_by_child_id: int | None = None,
    ein: str | None = None,
    tax_exempt_verified: bool = False,
    verified_by_parent: str | None = None,
    website_live: bool | None = None,
    seed_charity: bool = False,
) -> int:
    cleaned_name = name.strip()
    if not cleaned_name:
        raise ValueError("Charity name is required")
    normalized_website = _normalize_website(website)
    live = check_website_live(normalized_website) if website_live is None else website_live
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO charities (
                    name,
                    website,
                    ein,
                    website_live,
                    tax_exempt_verified,
                    verified_by_parent,
                    created_by_child_id,
                    seed_charity
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cleaned_name,
                    normalized_website,
                    ein.strip() if ein else None,
                    1 if live else 0,
                    1 if tax_exempt_verified else 0,
                    verified_by_parent.strip() if verified_by_parent else None,
                    created_by_child_id,
                    1 if seed_charity else 0,
                ),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError as err:
            raise ValueError(f"Charity already exists: {cleaned_name}") from err


def list_charities(limit: int = 200) -> Iterable[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                ch.id,
                ch.name,
                ch.website,
                ch.ein,
                ch.website_live,
                ch.tax_exempt_verified,
                ch.verified_by_parent,
                ch.created_by_child_id,
                c.name AS created_by_child_name,
                ch.seed_charity,
                ch.created_at
            FROM charities ch
            LEFT JOIN children c ON c.id = ch.created_by_child_id
            ORDER BY ch.tax_exempt_verified DESC, ch.seed_charity DESC, ch.name ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def set_charity_tax_exempt_verified(charity_id: int, verified_by_parent: str) -> bool:
    reviewer = verified_by_parent.strip()
    if not reviewer:
        raise ValueError("Parent name is required")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE charities
            SET tax_exempt_verified = 1,
                verified_by_parent = ?
            WHERE id = ?
            """,
            (reviewer, charity_id),
        )
        return cursor.rowcount > 0


def recheck_charity_website(charity_id: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT website FROM charities WHERE id = ?",
            (charity_id,),
        ).fetchone()
        if not row:
            raise ValueError("Charity not found")
        live = check_website_live(str(row["website"]))
        conn.execute(
            "UPDATE charities SET website_live = ? WHERE id = ?",
            (1 if live else 0, charity_id),
        )
        return live


def create_donation_pledge(child_id: int, charity_id: int, amount: float, note: str = "") -> int:
    value = float(amount)
    if value <= 0:
        raise ValueError("Donation amount must be greater than zero")
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT 1 FROM charities WHERE id = ?",
            (charity_id,),
        ).fetchone()
        if not exists:
            raise ValueError("Charity not found")
        cursor = conn.execute(
            """
            INSERT INTO donation_pledges (
                child_id,
                charity_id,
                amount,
                note,
                status
            )
            VALUES (?, ?, ?, ?, 'pending_parent')
            """,
            (child_id, charity_id, value, note.strip()),
        )
        return int(cursor.lastrowid)


def list_donation_pledges(
    child_id: int | None = None,
    status: str | None = None,
    limit: int = 200,
) -> Iterable[dict]:
    query = """
        SELECT
            dp.id,
            dp.child_id,
            c.name AS child_name,
            dp.charity_id,
            ch.name AS charity_name,
            ch.website AS charity_website,
            dp.amount,
            dp.note,
            dp.status,
            dp.created_at,
            dp.completed_at,
            dp.completed_by
        FROM donation_pledges dp
        JOIN children c ON c.id = dp.child_id
        JOIN charities ch ON ch.id = dp.charity_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("dp.child_id = ?")
        params.append(child_id)
    if status:
        where.append("dp.status = ?")
        params.append(status)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY dp.created_at DESC, dp.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def total_completed_donations(child_id: int | None = None) -> float:
    query = """
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM donation_pledges
        WHERE status = 'completed'
    """
    params: tuple[object, ...] = ()
    if child_id is not None:
        query += " AND child_id = ?"
        params = (child_id,)
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
        return float(row["total"]) if row else 0.0


def total_interest_earned(child_id: int | None = None) -> float:
    query = """
        SELECT COALESCE(SUM(interest_amount), 0) AS total
        FROM interest_accruals
    """
    params: tuple[object, ...] = ()
    if child_id is not None:
        query += " WHERE child_id = ?"
        params = (child_id,)
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
        return float(row["total"]) if row else 0.0


def list_interest_accruals(
    child_id: int | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
    limit: int = 500,
) -> Iterable[dict]:
    query = """
        SELECT
            ia.id,
            ia.child_id,
            c.name AS child_name,
            ia.accrual_date,
            ia.rate_percent,
            ia.opening_balance,
            ia.interest_amount,
            ia.created_at
        FROM interest_accruals ia
        JOIN children c ON c.id = ia.child_id
    """
    params: list[object] = []
    where: list[str] = []
    if child_id is not None:
        where.append("ia.child_id = ?")
        params.append(child_id)
    if date_from:
        where.append("date(ia.accrual_date) >= date(?)")
        params.append(_as_date(date_from).isoformat())
    if date_to:
        where.append("date(ia.accrual_date) <= date(?)")
        params.append(_as_date(date_to).isoformat())
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY ia.accrual_date DESC, ia.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def create_reading_log(
    child_id: int,
    read_date: str | date,
    start_time: str,
    end_time: str,
    book_title: str,
    chapters: str,
) -> int:
    day = _as_date(read_date).isoformat()
    start = _as_hhmm(start_time)
    end = _as_hhmm(end_time)
    if end <= start:
        raise ValueError("End time must be after start time")
    title = book_title.strip()
    chapter_text = chapters.strip()
    if not title:
        raise ValueError("Book title is required")
    if not chapter_text:
        raise ValueError("Chapters are required")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO reading_logs (
                child_id, read_date, start_time, end_time, book_title, chapters, status
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending_questions')
            """,
            (child_id, day, start, end, title, chapter_text),
        )
        return int(cursor.lastrowid)


def get_reading_log(log_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                rl.id,
                rl.child_id,
                c.name AS child_name,
                rl.read_date,
                rl.start_time,
                rl.end_time,
                rl.book_title,
                rl.chapters,
                rl.question_1,
                rl.question_2,
                rl.answer_1,
                rl.answer_2,
                rl.score,
                rl.passed,
                rl.status,
                rl.chatbot_provider,
                rl.parent_override_by,
                rl.parent_override_at,
                rl.credit_completion_id,
                rl.created_at,
                rl.evaluated_at
            FROM reading_logs rl
            JOIN children c ON c.id = rl.child_id
            WHERE rl.id = ?
            """,
            (log_id,),
        ).fetchone()
        return dict(row) if row else None


def list_reading_logs(
    child_id: int | None = None,
    status: str | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
    limit: int = 50,
) -> Iterable[dict]:
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("rl.child_id = ?")
        params.append(child_id)
    if status:
        where.append("rl.status = ?")
        params.append(status.strip())
    if date_from:
        where.append("date(rl.read_date) >= date(?)")
        params.append(_as_date(date_from).isoformat())
    if date_to:
        where.append("date(rl.read_date) <= date(?)")
        params.append(_as_date(date_to).isoformat())
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                rl.id,
                rl.child_id,
                c.name AS child_name,
                rl.read_date,
                rl.start_time,
                rl.end_time,
                rl.book_title,
                rl.chapters,
                rl.question_1,
                rl.question_2,
                rl.answer_1,
                rl.answer_2,
                rl.score,
                rl.passed,
                rl.status,
                rl.chatbot_provider,
                rl.parent_override_by,
                rl.parent_override_at,
                rl.credit_completion_id,
                rl.created_at,
                rl.evaluated_at
            FROM reading_logs rl
            JOIN children c ON c.id = rl.child_id
            """ + where_sql + """
            ORDER BY rl.read_date DESC, rl.id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]


def set_reading_log_questions(log_id: int, question_1: str, question_2: str, chatbot_provider: str = "") -> bool:
    q1 = question_1.strip()
    q2 = question_2.strip()
    if not q1 or not q2:
        raise ValueError("Both quiz questions are required")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE reading_logs
            SET question_1 = ?,
                question_2 = ?,
                chatbot_provider = ?,
                status = 'awaiting_answers'
            WHERE id = ?
            """,
            (q1, q2, chatbot_provider.strip(), log_id),
        )
        return cursor.rowcount > 0


def update_reading_log_quiz_result(
    log_id: int,
    answer_1: str,
    answer_2: str,
    passed: bool,
    score: float,
) -> bool:
    a1 = answer_1.strip()
    a2 = answer_2.strip()
    if not a1 or not a2:
        raise ValueError("Both answers are required")
    normalized_score = max(0.0, min(float(score), 1.0))
    status = "passed" if passed else "failed"
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE reading_logs
            SET answer_1 = ?,
                answer_2 = ?,
                passed = ?,
                score = ?,
                status = ?,
                evaluated_at = datetime('now')
            WHERE id = ?
              AND status IN ('awaiting_answers', 'failed')
            """,
            (a1, a2, 1 if passed else 0, normalized_score, status, log_id),
        )
        return cursor.rowcount > 0


def _find_reading_credit_task_id() -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id
            FROM tasks
            WHERE active = 1
              AND lower(name) IN ('read', 'reading', 'read book', 'reading practice')
            ORDER BY CASE WHEN rank = 'required' THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return int(row["id"])
        fallback = conn.execute(
            """
            SELECT id
            FROM tasks
            WHERE active = 1
              AND lower(name) LIKE '%read%'
            ORDER BY CASE WHEN rank = 'required' THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """
        ).fetchone()
        if fallback:
            return int(fallback["id"])
    raise ValueError("No active reading task found. Add a task named Read or Reading in Parent Panel.")


def award_reading_log_credit(log_id: int, reviewed_by: str = "Reading Bot") -> int:
    log = get_reading_log(log_id)
    if not log:
        raise ValueError("Reading log not found")
    if int(log.get("passed") or 0) != 1:
        raise ValueError("Reading quiz not passed yet")
    existing = log.get("credit_completion_id")
    if existing:
        return int(existing)
    task_id = _find_reading_credit_task_id()
    completion_id = record_task_completion(
        child_id=int(log["child_id"]),
        task_id=task_id,
        completion_note=f"Reading quiz passed for {log['book_title']} ({log['chapters']}) [log #{log_id}]",
        task_instance_id=None,
    )
    review_task_completion(
        completion_id=completion_id,
        decision="approved",
        reviewed_by=reviewed_by,
        review_note="Auto-approved after reading quiz pass",
    )
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE reading_logs
            SET credit_completion_id = ?
            WHERE id = ?
            """,
            (completion_id, log_id),
        )
    return completion_id


def parent_override_reading_credit(log_id: int, reviewed_by: str) -> int:
    reviewer = reviewed_by.strip()
    if not reviewer:
        raise ValueError("Parent name is required")
    log = get_reading_log(log_id)
    if not log:
        raise ValueError("Reading log not found")
    if log.get("credit_completion_id"):
        return int(log["credit_completion_id"])
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE reading_logs
            SET passed = 1,
                score = CASE WHEN score < 0.7 THEN 0.7 ELSE score END,
                status = 'passed',
                evaluated_at = COALESCE(evaluated_at, datetime('now')),
                parent_override_by = ?,
                parent_override_at = datetime('now')
            WHERE id = ?
            """,
            (reviewer, log_id),
        )
    return award_reading_log_credit(log_id, reviewed_by=reviewer)


def complete_donation_pledge(pledge_id: int, completed_by: str) -> bool:
    reviewer = completed_by.strip()
    if not reviewer:
        raise ValueError("Parent name is required")
    with get_connection() as conn:
        pledge = conn.execute(
            """
            SELECT
                dp.id,
                dp.child_id,
                dp.charity_id,
                dp.amount,
                dp.status,
                ch.name AS charity_name
            FROM donation_pledges dp
            JOIN charities ch ON ch.id = dp.charity_id
            WHERE dp.id = ?
            """,
            (pledge_id,),
        ).fetchone()
        if not pledge:
            raise ValueError("Donation pledge not found")
        if pledge["status"] != "pending_parent":
            return False

        balance_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS balance
            FROM ledger_entries
            WHERE child_id = ? AND asset_type = 'allowance'
            """,
            (pledge["child_id"],),
        ).fetchone()
        balance = float(balance_row["balance"]) if balance_row else 0.0
        amount = float(pledge["amount"])
        if balance < amount:
            raise ValueError(
                f"Insufficient allowance balance: has {balance:.2f}, needs {amount:.2f}"
            )

        conn.execute(
            """
            INSERT INTO ledger_entries (
                child_id,
                asset_type,
                amount,
                source_type,
                note
            )
            VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
            """,
            (
                pledge["child_id"],
                -amount,
                f"Donation completed to {pledge['charity_name']} (pledge #{pledge_id})",
            ),
        )
        cursor = conn.execute(
            """
            UPDATE donation_pledges
            SET status = 'completed',
                completed_at = datetime('now'),
                completed_by = ?
            WHERE id = ? AND status = 'pending_parent'
            """,
            (reviewer, pledge_id),
        )
        return cursor.rowcount > 0


def get_allowance_balance(child_id: int) -> float:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS balance
            FROM ledger_entries
            WHERE child_id = ? AND asset_type = 'allowance'
            """,
            (child_id,),
        ).fetchone()
        return float(row["balance"]) if row else 0.0


def get_pending_wallet_payout_total(child_id: int) -> float:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM wallet_payouts
            WHERE child_id = ? AND status = 'pending_parent'
            """,
            (child_id,),
        ).fetchone()
        return float(row["total"]) if row else 0.0


def get_available_allowance_for_payout(child_id: int) -> float:
    return round(get_allowance_balance(child_id) - get_pending_wallet_payout_total(child_id), 2)


def request_wallet_payout(child_id: int, amount: float, note: str = "") -> int:
    payout_amount = round(float(amount), 2)
    if payout_amount <= 0:
        raise ValueError("Payout amount must be greater than zero")
    available = get_available_allowance_for_payout(child_id)
    if payout_amount > available:
        raise ValueError(f"Insufficient available allowance: has {available:.2f}, needs {payout_amount:.2f}")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO wallet_payouts (child_id, amount, note, status)
            VALUES (?, ?, ?, 'pending_parent')
            """,
            (child_id, payout_amount, note.strip()),
        )
        return int(cursor.lastrowid)


def mark_wallet_payout_sent(payout_id: int, sent_by: str, transfer_reference: str = "") -> bool:
    by = sent_by.strip()
    if not by:
        raise ValueError("Parent name is required")
    with get_connection() as conn:
        payout = conn.execute(
            """
            SELECT id, child_id, amount, status
            FROM wallet_payouts
            WHERE id = ?
            """,
            (payout_id,),
        ).fetchone()
        if not payout:
            raise ValueError("Wallet payout not found")
        if payout["status"] != "pending_parent":
            return False

        available = get_available_allowance_for_payout(int(payout["child_id"])) + float(payout["amount"])
        amount = float(payout["amount"])
        if amount > available:
            raise ValueError(
                f"Insufficient allowance to complete payout: has {available:.2f}, needs {amount:.2f}"
            )

        conn.execute(
            """
            INSERT INTO ledger_entries (
                child_id,
                asset_type,
                amount,
                source_type,
                note
            )
            VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
            """,
            (
                payout["child_id"],
                -amount,
                f"Wallet payout sent (payout #{payout_id})",
            ),
        )
        cursor = conn.execute(
            """
            UPDATE wallet_payouts
            SET status = 'sent',
                sent_at = datetime('now'),
                sent_by = ?,
                transfer_reference = ?
            WHERE id = ? AND status = 'pending_parent'
            """,
            (by, transfer_reference.strip(), payout_id),
        )
        return cursor.rowcount > 0


def list_wallet_payouts(child_id: int | None = None, status: str | None = None, limit: int = 300) -> Iterable[dict]:
    query = """
        SELECT
            wp.id,
            wp.child_id,
            c.name AS child_name,
            wp.amount,
            wp.note,
            wp.status,
            wp.created_at,
            wp.sent_at,
            wp.sent_by,
            wp.transfer_reference
        FROM wallet_payouts wp
        JOIN children c ON c.id = wp.child_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("wp.child_id = ?")
        params.append(child_id)
    if status:
        where.append("wp.status = ?")
        params.append(status)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY wp.created_at DESC, wp.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def add_service_organization(name: str, website: str = "", created_by_child_id: int | None = None) -> int:
    cleaned_name = name.strip()
    if not cleaned_name:
        raise ValueError("Organization name is required")
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO service_organizations (name, website, active, created_by_child_id)
                VALUES (?, ?, 1, ?)
                """,
                (cleaned_name, website.strip(), created_by_child_id),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError as err:
            raise ValueError(f"Organization already exists: {cleaned_name}") from err


def list_service_organizations(active_only: bool = True, limit: int = 300) -> Iterable[dict]:
    query = """
        SELECT
            so.id,
            so.name,
            so.website,
            so.active,
            so.created_by_child_id,
            c.name AS created_by_child_name,
            so.created_at
        FROM service_organizations so
        LEFT JOIN children c ON c.id = so.created_by_child_id
    """
    params: list[object] = []
    if active_only:
        query += " WHERE so.active = 1"
    query += " ORDER BY so.name ASC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def submit_service_hours(
    child_id: int,
    organization_id: int,
    hours: float,
    service_date: str | date | None = None,
    note: str = "",
) -> int:
    value = round(float(hours), 2)
    if value <= 0:
        raise ValueError("Service hours must be greater than zero")
    service_day = _as_date(service_date or date.today()).isoformat()
    with get_connection() as conn:
        org = conn.execute(
            "SELECT id FROM service_organizations WHERE id = ? AND active = 1",
            (organization_id,),
        ).fetchone()
        if not org:
            raise ValueError("Organization not found or inactive")
        cursor = conn.execute(
            """
            INSERT INTO service_entries (
                child_id,
                organization_id,
                hours,
                service_date,
                note,
                status
            )
            VALUES (?, ?, ?, ?, ?, 'pending_parent')
            """,
            (child_id, organization_id, value, service_day, note.strip()),
        )
        return int(cursor.lastrowid)


def list_service_entries(
    child_id: int | None = None,
    status: str | None = None,
    limit: int = 300,
) -> Iterable[dict]:
    query = """
        SELECT
            se.id,
            se.child_id,
            c.name AS child_name,
            se.organization_id,
            so.name AS organization_name,
            so.website AS organization_website,
            se.hours,
            se.service_date,
            se.note,
            se.status,
            se.reviewed_by,
            se.review_note,
            se.created_at,
            se.reviewed_at
        FROM service_entries se
        JOIN children c ON c.id = se.child_id
        JOIN service_organizations so ON so.id = se.organization_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("se.child_id = ?")
        params.append(child_id)
    if status:
        where.append("se.status = ?")
        params.append(status)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY se.service_date DESC, se.id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def get_service_credit_rates() -> dict[str, float]:
    allowance = float(get_setting("service_allowance_per_hour") or "0")
    screen_minutes = float(get_setting("service_screen_minutes_per_hour") or "0")
    return {
        "allowance_per_hour": allowance,
        "screen_minutes_per_hour": screen_minutes,
    }


def set_service_credit_rates(allowance_per_hour: float, screen_minutes_per_hour: float) -> None:
    if allowance_per_hour < 0 or screen_minutes_per_hour < 0:
        raise ValueError("Service rates must be >= 0")
    set_setting("service_allowance_per_hour", str(round(float(allowance_per_hour), 2)))
    set_setting("service_screen_minutes_per_hour", str(round(float(screen_minutes_per_hour), 2)))


def _validate_hhmm(value: str) -> str:
    cleaned = value.strip()
    try:
        datetime.strptime(cleaned, "%H:%M")
    except ValueError as err:
        raise ValueError("Time must be in HH:MM format (24-hour clock)") from err
    return cleaned


def upsert_child_house_rules(
    child_id: int,
    weekday_screen_off: str,
    weekday_bedtime: str,
    weekend_screen_off: str,
    weekend_bedtime: str,
) -> bool:
    ws = _validate_hhmm(weekday_screen_off)
    wb = _validate_hhmm(weekday_bedtime)
    wes = _validate_hhmm(weekend_screen_off)
    web = _validate_hhmm(weekend_bedtime)
    with get_connection() as conn:
        child = conn.execute("SELECT 1 FROM children WHERE id = ? AND active = 1", (child_id,)).fetchone()
        if not child:
            return False
        conn.execute(
            """
            INSERT INTO child_house_rules (
                child_id, weekday_screen_off, weekday_bedtime, weekend_screen_off, weekend_bedtime
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(child_id) DO UPDATE SET
                weekday_screen_off = excluded.weekday_screen_off,
                weekday_bedtime = excluded.weekday_bedtime,
                weekend_screen_off = excluded.weekend_screen_off,
                weekend_bedtime = excluded.weekend_bedtime
            """,
            (child_id, ws, wb, wes, web),
        )
        return True


def list_child_house_rules(active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            c.id AS child_id,
            c.name AS child_name,
            chr.weekday_screen_off,
            chr.weekday_bedtime,
            chr.weekend_screen_off,
            chr.weekend_bedtime
        FROM children c
        LEFT JOIN child_house_rules chr ON chr.child_id = c.id
    """
    if active_only:
        query += " WHERE c.active = 1"
    query += " ORDER BY c.name ASC"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def get_after_school_reminders() -> list[str]:
    raw = get_setting("after_school_reminders") or ""
    parts = [item.strip() for item in raw.split(",")]
    return [p for p in parts if p]


def set_after_school_reminders(reminders: list[str] | str) -> None:
    if isinstance(reminders, str):
        raw_items = reminders.replace("\r", "\n").replace(",", "\n").split("\n")
        cleaned = [item.strip() for item in raw_items if item.strip()]
    else:
        cleaned = [item.strip() for item in reminders if item.strip()]
    if not cleaned:
        raise ValueError("At least one reminder is required")
    set_setting("after_school_reminders", ",".join(cleaned))


def get_screen_time_allotment_minutes(on_date: str | date | None = None) -> int:
    d = _as_date(on_date or date.today())
    is_weekend = d.weekday() in (5, 6)
    key = "screen_allot_weekend_minutes" if is_weekend else "screen_allot_weekday_minutes"
    raw = get_setting(key) or ("120" if is_weekend else "60")
    try:
        value = int(float(raw))
    except ValueError:
        value = 120 if is_weekend else 60
    return max(value, 0)


def upsert_child_app_limit(child_id: int, app_name: str, minutes_per_day: int, active: bool = True) -> int:
    cleaned = app_name.strip()
    if not cleaned:
        raise ValueError("App name is required")
    minutes = int(minutes_per_day)
    if minutes < 0:
        raise ValueError("minutes_per_day must be >= 0")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO child_app_limits (child_id, app_name, minutes_per_day, active)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(child_id, app_name) DO UPDATE SET
                minutes_per_day = excluded.minutes_per_day,
                active = excluded.active
            """,
            (child_id, cleaned, minutes, 1 if active else 0),
        )
        row = conn.execute(
            """
            SELECT id
            FROM child_app_limits
            WHERE child_id = ? AND app_name = ?
            """,
            (child_id, cleaned),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to save app limit")
        return int(row["id"])


def list_child_app_limits(child_id: int | None = None, active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            cal.id,
            cal.child_id,
            c.name AS child_name,
            cal.app_name,
            cal.minutes_per_day,
            cal.active,
            cal.created_at
        FROM child_app_limits cal
        JOIN children c ON c.id = cal.child_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("cal.child_id = ?")
        params.append(child_id)
    if active_only:
        where.append("cal.active = 1")
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY c.name ASC, cal.app_name ASC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def add_concert_goal(
    child_id: int,
    artist_name: str,
    event_name: str,
    event_date: str | date,
    low_price: float,
    high_price: float,
    venue_name: str = "",
    city: str = "",
    state_code: str = "",
    currency: str = "USD",
    ticket_url: str = "",
    kid_share_percent: int = 80,
) -> int:
    artist = artist_name.strip()
    event = event_name.strip()
    if not artist or not event:
        raise ValueError("Artist and event name are required")
    event_day = _as_date(event_date).isoformat()
    low = float(low_price)
    high = float(high_price)
    if low < 0 or high < low:
        raise ValueError("Invalid price range")
    pct = int(kid_share_percent)
    if pct < 1 or pct > 100:
        raise ValueError("kid_share_percent must be between 1 and 100")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO concert_goals (
                child_id,
                artist_name,
                event_name,
                event_date,
                venue_name,
                city,
                state_code,
                low_price,
                high_price,
                currency,
                ticket_url,
                kid_share_percent,
                active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                child_id,
                artist,
                event,
                event_day,
                venue_name.strip(),
                city.strip(),
                state_code.strip(),
                low,
                high,
                (currency or "USD").strip().upper(),
                ticket_url.strip(),
                pct,
            ),
        )
        return int(cursor.lastrowid)


def list_concert_goals(child_id: int | None = None, active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            cg.id,
            cg.child_id,
            c.name AS child_name,
            cg.artist_name,
            cg.event_name,
            cg.event_date,
            cg.venue_name,
            cg.city,
            cg.state_code,
            cg.low_price,
            cg.high_price,
            cg.currency,
            cg.ticket_url,
            cg.kid_share_percent,
            cg.goal_status,
            cg.active,
            cg.created_at
        FROM concert_goals cg
        JOIN children c ON c.id = cg.child_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("cg.child_id = ?")
        params.append(child_id)
    if active_only:
        where.append("cg.active = 1")
        where.append("cg.goal_status = 'active'")
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY cg.event_date ASC, cg.id DESC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        result: list[dict] = []
        for row in rows:
            data = dict(row)
            low = float(data["low_price"])
            pct = int(data["kid_share_percent"])
            kid_target = round(low * (pct / 100.0), 2)
            parent_part = round(low - kid_target, 2)
            data["kid_target_amount"] = kid_target
            data["parent_share_amount"] = parent_part
            result.append(data)
        return result


def set_concert_goal_status(goal_id: int, status: str) -> bool:
    normalized = status.strip().lower()
    if normalized not in ("active", "purchased", "archived"):
        raise ValueError("Status must be active, purchased, or archived")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE concert_goals
            SET goal_status = ?,
                active = CASE WHEN ? = 'archived' THEN 0 ELSE 1 END
            WHERE id = ?
            """,
            (normalized, normalized, goal_id),
        )
        return cursor.rowcount > 0


def list_child_concert_goal_progress(child_id: int) -> Iterable[dict]:
    goals = list_concert_goals(child_id=child_id, active_only=True)
    allowance = get_allowance_balance(child_id)
    result: list[dict] = []
    for goal in goals:
        target = float(goal["kid_target_amount"])
        pct = 100.0 if target <= 0 else min(round((allowance / target) * 100.0, 2), 100.0)
        goal["child_allowance_balance"] = round(allowance, 2)
        goal["progress_percent"] = pct
        goal["eligible"] = allowance >= target
        result.append(goal)
    return result


def add_adventure_park_catalog(
    name: str,
    region: str,
    category: str,
    website: str,
    low_price: float,
    high_price: float,
    currency: str = "USD",
) -> int:
    park_name = name.strip()
    park_region = region.strip()
    park_category = category.strip() or "theme_park"
    if not park_name or not park_region:
        raise ValueError("Park name and region are required")
    low = float(low_price)
    high = float(high_price)
    if low < 0 or high < low:
        raise ValueError("Invalid price range")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO adventure_park_catalog (
                name,
                region,
                category,
                website,
                low_price,
                high_price,
                currency,
                active,
                seed_park
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0)
            """,
            (
                park_name,
                park_region,
                park_category,
                website.strip(),
                low,
                high,
                (currency or "USD").strip().upper(),
            ),
        )
        return int(cursor.lastrowid)


def list_adventure_park_catalog(active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            ap.id,
            ap.name,
            ap.region,
            ap.category,
            ap.website,
            ap.low_price,
            ap.high_price,
            ap.currency,
            ap.active,
            ap.seed_park,
            ap.created_at
        FROM adventure_park_catalog ap
    """
    params: list[object] = []
    if active_only:
        query += " WHERE ap.active = 1"
    query += " ORDER BY ap.region ASC, ap.name ASC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def add_adventure_goal(
    child_id: int,
    park_name: str,
    ticket_name: str,
    target_date: str | date,
    low_price: float,
    high_price: float,
    region: str = "",
    category: str = "theme_park",
    currency: str = "USD",
    ticket_url: str = "",
    kid_share_percent: int = 80,
) -> int:
    park = park_name.strip()
    ticket = ticket_name.strip()
    if not park or not ticket:
        raise ValueError("Park name and ticket name are required")
    event_day = _as_date(target_date).isoformat()
    low = float(low_price)
    high = float(high_price)
    if low < 0 or high < low:
        raise ValueError("Invalid price range")
    pct = int(kid_share_percent)
    if pct < 1 or pct > 100:
        raise ValueError("kid_share_percent must be between 1 and 100")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO adventure_goals (
                child_id,
                park_name,
                ticket_name,
                region,
                category,
                target_date,
                low_price,
                high_price,
                currency,
                ticket_url,
                kid_share_percent,
                active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                child_id,
                park,
                ticket,
                region.strip(),
                (category.strip() or "theme_park"),
                event_day,
                low,
                high,
                (currency or "USD").strip().upper(),
                ticket_url.strip(),
                pct,
            ),
        )
        return int(cursor.lastrowid)


def list_adventure_goals(child_id: int | None = None, active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            ag.id,
            ag.child_id,
            c.name AS child_name,
            ag.park_name,
            ag.ticket_name,
            ag.region,
            ag.category,
            ag.target_date,
            ag.low_price,
            ag.high_price,
            ag.currency,
            ag.ticket_url,
            ag.kid_share_percent,
            ag.goal_status,
            ag.active,
            ag.created_at
        FROM adventure_goals ag
        JOIN children c ON c.id = ag.child_id
    """
    where: list[str] = []
    params: list[object] = []
    if child_id is not None:
        where.append("ag.child_id = ?")
        params.append(child_id)
    if active_only:
        where.append("ag.active = 1")
        where.append("ag.goal_status = 'active'")
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY ag.target_date ASC, ag.id DESC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        result: list[dict] = []
        for row in rows:
            data = dict(row)
            low = float(data["low_price"])
            pct = int(data["kid_share_percent"])
            kid_target = round(low * (pct / 100.0), 2)
            parent_part = round(low - kid_target, 2)
            data["kid_target_amount"] = kid_target
            data["parent_share_amount"] = parent_part
            result.append(data)
        return result


def set_adventure_goal_status(goal_id: int, status: str) -> bool:
    normalized = status.strip().lower()
    if normalized not in ("active", "purchased", "archived"):
        raise ValueError("Status must be active, purchased, or archived")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE adventure_goals
            SET goal_status = ?,
                active = CASE WHEN ? = 'archived' THEN 0 ELSE 1 END
            WHERE id = ?
            """,
            (normalized, normalized, goal_id),
        )
        return cursor.rowcount > 0


def list_child_adventure_goal_progress(child_id: int) -> Iterable[dict]:
    goals = list_adventure_goals(child_id=child_id, active_only=True)
    allowance = get_allowance_balance(child_id)
    result: list[dict] = []
    for goal in goals:
        target = float(goal["kid_target_amount"])
        pct = 100.0 if target <= 0 else min(round((allowance / target) * 100.0, 2), 100.0)
        goal["child_allowance_balance"] = round(allowance, 2)
        goal["progress_percent"] = pct
        goal["eligible"] = allowance >= target
        result.append(goal)
    return result


def save_ios_usage_report(
    child_name: str,
    usage_date: str | date,
    total_minutes: int,
    per_app_minutes: dict[str, int],
) -> int:
    name = child_name.strip()
    if not name:
        raise ValueError("child_name is required")
    day = _as_date(usage_date).isoformat()
    total = int(total_minutes)
    if total < 0:
        raise ValueError("total_minutes must be >= 0")
    cleaned: dict[str, int] = {}
    for app, minutes in per_app_minutes.items():
        app_name = str(app).strip()
        if not app_name:
            continue
        m = int(minutes)
        if m < 0:
            raise ValueError("per-app minutes must be >= 0")
        cleaned[app_name] = m
    payload = json.dumps(cleaned, sort_keys=True)
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ios_usage_reports (
                child_name,
                usage_date,
                total_minutes,
                per_app_json
            )
            VALUES (?, ?, ?, ?)
            """,
            (name, day, total, payload),
        )
        return int(cursor.lastrowid)


def list_ios_usage_reports(limit: int = 200) -> Iterable[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, child_name, usage_date, total_minutes, per_app_json, source, created_at
            FROM ios_usage_reports
            ORDER BY usage_date DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def search_ticketmaster_events(
    artist_keyword: str,
    city: str = "",
    state_code: str = "",
    size: int = 20,
) -> list[dict]:
    api_key = os.environ.get("TICKETMASTER_API_KEY", "").strip()
    if not api_key:
        raise ValueError("TICKETMASTER_API_KEY is not set")
    keyword = artist_keyword.strip()
    if not keyword:
        raise ValueError("Artist keyword is required")
    params = {
        "apikey": api_key,
        "keyword": keyword,
        "classificationName": "music",
        "sort": "date,asc",
        "size": str(max(1, min(size, 50))),
    }
    if city.strip():
        params["city"] = city.strip()
    if state_code.strip():
        params["stateCode"] = state_code.strip()
    url = f"https://app.ticketmaster.com/discovery/v2/events.json?{urlencode(params)}"
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=15) as resp:
        payload = resp.read().decode("utf-8")
    data = json.loads(payload)
    events = data.get("_embedded", {}).get("events", [])
    results: list[dict] = []
    for ev in events:
        dates = ev.get("dates", {}).get("start", {})
        date_local = dates.get("localDate", "")
        venue = ((ev.get("_embedded", {}) or {}).get("venues", [{}])[0]) if ev.get("_embedded") else {}
        prices = ev.get("priceRanges") or []
        low_price = None
        high_price = None
        currency = "USD"
        if prices:
            low_price = prices[0].get("min")
            high_price = prices[0].get("max")
            currency = prices[0].get("currency") or "USD"
        results.append(
            {
                "artist_name": keyword,
                "event_name": str(ev.get("name") or ""),
                "event_date": str(date_local or ""),
                "venue_name": str(venue.get("name") or ""),
                "city": str((venue.get("city") or {}).get("name") or ""),
                "state_code": str((venue.get("state") or {}).get("stateCode") or ""),
                "low_price": float(low_price) if low_price is not None else None,
                "high_price": float(high_price) if high_price is not None else None,
                "currency": str(currency),
                "ticket_url": str(ev.get("url") or ""),
            }
        )
    return results


def add_child_activity(
    child_id: int,
    activity_name: str,
    start_time: str,
    end_time: str | None = None,
    category: str = "activity",
    day_of_week: int | None = None,
    specific_date: str | date | None = None,
    location: str = "",
    notes: str = "",
    notify_enabled: bool = False,
    notify_minutes_before: int = 30,
    notify_channels: str = "email",
) -> int:
    if not activity_name.strip():
        raise ValueError("Activity name is required")
    start = _validate_hhmm(start_time)
    end = _validate_hhmm(end_time) if end_time else None
    if specific_date is None and day_of_week is None:
        raise ValueError("Either day_of_week or specific_date is required")
    if day_of_week is not None and (day_of_week < 0 or day_of_week > 6):
        raise ValueError("day_of_week must be between 0 and 6")
    if specific_date is not None:
        day_of_week = None
    day = _as_date(specific_date).isoformat() if specific_date is not None else None
    channels = notify_channels.strip().lower() or "email"
    if channels not in ("email", "sms", "both"):
        raise ValueError("notify_channels must be email, sms, or both")
    minutes_before = int(notify_minutes_before)
    if minutes_before < 0 or minutes_before > 1440:
        raise ValueError("notify_minutes_before must be between 0 and 1440")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO child_activities (
                child_id,
                activity_name,
                category,
                day_of_week,
                specific_date,
                start_time,
                end_time,
                location,
                notes,
                notify_enabled,
                notify_minutes_before,
                notify_channels,
                active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                child_id,
                activity_name.strip(),
                category.strip() or "activity",
                day_of_week,
                day,
                start,
                end,
                location.strip(),
                notes.strip(),
                1 if notify_enabled else 0,
                minutes_before,
                channels,
            ),
        )
        return int(cursor.lastrowid)


def add_holiday(holiday_date: str | date, holiday_name: str, no_school: bool = True) -> int:
    if not holiday_name.strip():
        raise ValueError("Holiday name is required")
    day = _as_date(holiday_date).isoformat()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO holidays (holiday_date, holiday_name, no_school)
            VALUES (?, ?, ?)
            ON CONFLICT(holiday_date) DO UPDATE SET
                holiday_name = excluded.holiday_name,
                no_school = excluded.no_school
            """,
            (day, holiday_name.strip(), 1 if no_school else 0),
        )
        row = conn.execute(
            "SELECT id FROM holidays WHERE holiday_date = ?",
            (day,),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to save holiday")
        return int(row["id"])


def list_holidays(limit: int = 365) -> Iterable[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, holiday_date, holiday_name, no_school, created_at
            FROM holidays
            ORDER BY holiday_date ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_child_activities_for_date(child_id: int, on_date: str | date | None = None) -> Iterable[dict]:
    day = _as_date(on_date or date.today())
    day_iso = day.isoformat()
    weekday = day.weekday()
    with get_connection() as conn:
        holiday = conn.execute(
            """
            SELECT holiday_name, no_school
            FROM holidays
            WHERE holiday_date = ?
            """,
            (day_iso,),
        ).fetchone()
        no_school = bool(int(holiday["no_school"])) if holiday else False

        rows = conn.execute(
            """
            SELECT
                a.id,
                a.child_id,
                a.activity_name,
                a.category,
                a.day_of_week,
                a.specific_date,
                a.start_time,
                a.end_time,
                a.location,
                a.notes,
                a.notify_enabled,
                a.notify_minutes_before,
                a.notify_channels,
                a.active
            FROM child_activities a
            WHERE a.child_id = ?
              AND a.active = 1
              AND (
                a.specific_date = ?
                OR (a.specific_date IS NULL AND a.day_of_week = ?)
              )
            ORDER BY a.start_time ASC, a.activity_name ASC
            """,
            (child_id, day_iso, weekday),
        ).fetchall()
        result = [dict(row) for row in rows]
        if no_school:
            result = [r for r in result if str(r.get("category")) != "school"]
        if holiday:
            result.insert(
                0,
                {
                    "id": None,
                    "child_id": child_id,
                    "activity_name": str(holiday["holiday_name"]),
                    "category": "holiday",
                    "day_of_week": weekday,
                    "specific_date": day_iso,
                    "start_time": "",
                    "end_time": "",
                    "location": "",
                    "notes": "No school today.",
                    "active": 1,
                },
            )
        return result


def list_child_activities(active_only: bool = True, limit: int = 500) -> Iterable[dict]:
    query = """
        SELECT
            a.id,
            a.child_id,
            c.name AS child_name,
            a.activity_name,
            a.category,
            a.day_of_week,
            a.specific_date,
            a.start_time,
            a.end_time,
            a.location,
            a.notes,
            a.notify_enabled,
            a.notify_minutes_before,
            a.notify_channels,
            a.active,
            a.created_at
        FROM child_activities a
        JOIN children c ON c.id = a.child_id
    """
    params: list[object] = []
    if active_only:
        query += " WHERE a.active = 1"
    query += " ORDER BY c.name ASC, COALESCE(a.specific_date, ''), a.day_of_week ASC, a.start_time ASC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def update_activity_notification_settings(
    activity_id: int,
    notify_enabled: bool,
    notify_minutes_before: int,
    notify_channels: str,
) -> bool:
    channels = notify_channels.strip().lower() or "email"
    if channels not in ("email", "sms", "both"):
        raise ValueError("notify_channels must be email, sms, or both")
    minutes_before = int(notify_minutes_before)
    if minutes_before < 0 or minutes_before > 1440:
        raise ValueError("notify_minutes_before must be between 0 and 1440")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE child_activities
            SET notify_enabled = ?,
                notify_minutes_before = ?,
                notify_channels = ?
            WHERE id = ?
            """,
            (1 if notify_enabled else 0, minutes_before, channels, activity_id),
        )
        return cursor.rowcount > 0


def list_pending_activity_notifications(now_at: datetime | None = None) -> list[dict]:
    now = now_at or datetime.now()
    day = now.date().isoformat()
    weekday = now.weekday()
    pending: list[dict] = []
    with get_connection() as conn:
        holiday = conn.execute(
            "SELECT no_school FROM holidays WHERE holiday_date = ?",
            (day,),
        ).fetchone()
        no_school = bool(int(holiday["no_school"])) if holiday else False
        rows = conn.execute(
            """
            SELECT
                a.id AS activity_id,
                a.child_id,
                c.name AS child_name,
                c.email AS child_email,
                c.text_number AS child_text_number,
                a.activity_name,
                a.category,
                a.start_time,
                a.end_time,
                a.notify_minutes_before,
                a.notify_channels
            FROM child_activities a
            JOIN children c ON c.id = a.child_id
            WHERE a.active = 1
              AND a.notify_enabled = 1
              AND (
                a.specific_date = ?
                OR (a.specific_date IS NULL AND a.day_of_week = ?)
              )
            ORDER BY a.start_time ASC, a.id ASC
            """,
            (day, weekday),
        ).fetchall()
        for row in rows:
            category = str(row["category"] or "")
            if no_school and category == "school":
                continue
            start_text = str(row["start_time"] or "").strip()
            if not start_text:
                continue
            try:
                start_dt = datetime.strptime(f"{day} {start_text}", "%Y-%m-%d %H:%M")
            except ValueError:
                continue
            notify_at = start_dt - timedelta(minutes=int(row["notify_minutes_before"]))
            if now < notify_at or now > start_dt:
                continue

            channels = str(row["notify_channels"] or "email")
            channel_list = ["email", "sms"] if channels == "both" else [channels]
            for channel in channel_list:
                log = conn.execute(
                    """
                    SELECT 1
                    FROM activity_notification_log
                    WHERE activity_id = ? AND occurrence_date = ? AND channel = ?
                    """,
                    (row["activity_id"], day, channel),
                ).fetchone()
                if log:
                    continue
                pending.append(
                    {
                        "activity_id": int(row["activity_id"]),
                        "child_id": int(row["child_id"]),
                        "child_name": str(row["child_name"]),
                        "child_email": str(row["child_email"] or ""),
                        "child_text_number": str(row["child_text_number"] or ""),
                        "activity_name": str(row["activity_name"]),
                        "category": category,
                        "start_time": start_text,
                        "end_time": str(row["end_time"] or ""),
                        "occurrence_date": day,
                        "channel": channel,
                    }
                )
    return pending


def mark_activity_notification_sent(
    activity_id: int,
    occurrence_date: str | date,
    channel: str,
    sent_to: str = "",
) -> bool:
    if channel not in ("email", "sms"):
        raise ValueError("channel must be email or sms")
    day = _as_date(occurrence_date).isoformat()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO activity_notification_log (
                activity_id, occurrence_date, channel, sent_to
            )
            VALUES (?, ?, ?, ?)
            """,
            (activity_id, day, channel, sent_to.strip()),
        )
        return cursor.rowcount > 0


def log_activity_notification_attempt(
    activity_id: int,
    occurrence_date: str | date,
    channel: str,
    target: str,
    success: bool,
    error_text: str = "",
) -> int:
    if channel not in ("email", "sms"):
        raise ValueError("channel must be email or sms")
    day = _as_date(occurrence_date).isoformat()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO activity_notification_attempts (
                activity_id, occurrence_date, channel, target, success, error_text
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                activity_id,
                day,
                channel,
                target.strip(),
                1 if success else 0,
                error_text.strip(),
            ),
        )
        return int(cursor.lastrowid)


def list_activity_notification_attempts(limit: int = 200) -> Iterable[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                ana.id,
                ana.activity_id,
                c.name AS child_name,
                a.activity_name,
                ana.occurrence_date,
                ana.channel,
                ana.target,
                ana.success,
                ana.error_text,
                ana.created_at
            FROM activity_notification_attempts ana
            JOIN child_activities a ON a.id = ana.activity_id
            JOIN children c ON c.id = a.child_id
            ORDER BY ana.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def add_motd_library_message(message_text: str, category: str = "custom") -> int:
    cleaned = message_text.strip()
    if not cleaned:
        raise ValueError("Message text is required")
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO motd_library (message_text, category, active)
                VALUES (?, ?, 1)
                """,
                (cleaned, category.strip() or "custom"),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError as err:
            raise ValueError("Message already exists in library") from err


def list_motd_library(active_only: bool = True, limit: int = 300) -> Iterable[dict]:
    query = "SELECT id, message_text, category, active, created_at FROM motd_library"
    params: list[object] = []
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def set_message_of_the_day(
    target_date: str | date,
    set_by: str,
    message_text: str = "",
    library_id: int | None = None,
) -> int:
    day = _as_date(target_date).isoformat()
    by = set_by.strip()
    if not by:
        raise ValueError("Parent name is required")
    text = message_text.strip()
    lib_id = library_id
    with get_connection() as conn:
        if lib_id is not None:
            row = conn.execute(
                "SELECT id, message_text FROM motd_library WHERE id = ? AND active = 1",
                (lib_id,),
            ).fetchone()
            if not row:
                raise ValueError("MOTD library message not found")
            text = str(row["message_text"])
            lib_id = int(row["id"])
        if not text:
            raise ValueError("Message text is required")
        conn.execute(
            """
            INSERT INTO motd_schedule (target_date, message_text, library_id, set_by)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(target_date) DO UPDATE SET
                message_text = excluded.message_text,
                library_id = excluded.library_id,
                set_by = excluded.set_by,
                created_at = datetime('now')
            """,
            (day, text, lib_id, by),
        )
        row = conn.execute(
            "SELECT id FROM motd_schedule WHERE target_date = ?",
            (day,),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to save message of the day")
        return int(row["id"])


def get_message_of_the_day(target_date: str | date | None = None) -> dict | None:
    d = _as_date(target_date or date.today())
    day = d.isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, target_date, message_text, library_id, set_by, created_at
            FROM motd_schedule
            WHERE target_date = ?
            """,
            (day,),
        ).fetchone()
        if row:
            return dict(row)
        count_row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM motd_library WHERE active = 1",
        ).fetchone()
        total = int(count_row["cnt"]) if count_row else 0
        if total <= 0:
            return None
        # Spread daily defaults across the full library to avoid near-duplicate adjacent days.
        idx = ((d.year * 53) + (d.month * 31) + (d.day * 17)) % total
        fallback = conn.execute(
            """
            SELECT id, message_text
            FROM motd_library
            WHERE active = 1
            ORDER BY id ASC
            LIMIT 1 OFFSET ?
            """,
            (idx,),
        ).fetchone()
        if not fallback:
            return None
        return {
            "id": None,
            "target_date": day,
            "message_text": str(fallback["message_text"]),
            "library_id": int(fallback["id"]),
            "set_by": "system_default",
            "created_at": None,
        }


def add_fun_fact_library_fact(fact_text: str, category: str = "custom") -> int:
    cleaned = fact_text.strip()
    if not cleaned:
        raise ValueError("Fact text is required")
    with get_connection() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO fun_fact_library (fact_text, category, active)
                VALUES (?, ?, 1)
                """,
                (cleaned, category.strip() or "custom"),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError as err:
            raise ValueError("Fact already exists in library") from err


def list_fun_fact_library(active_only: bool = True, limit: int = 500) -> Iterable[dict]:
    query = "SELECT id, fact_text, category, active, created_at FROM fun_fact_library"
    params: list[object] = []
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def set_fun_fact_of_the_day(
    target_date: str | date,
    set_by: str,
    fact_text: str = "",
    library_id: int | None = None,
) -> int:
    day = _as_date(target_date).isoformat()
    by = set_by.strip()
    if not by:
        raise ValueError("Parent name is required")
    text = fact_text.strip()
    lib_id = library_id
    with get_connection() as conn:
        if lib_id is not None:
            row = conn.execute(
                "SELECT id, fact_text FROM fun_fact_library WHERE id = ? AND active = 1",
                (lib_id,),
            ).fetchone()
            if not row:
                raise ValueError("Fun fact library entry not found")
            text = str(row["fact_text"])
            lib_id = int(row["id"])
        if not text:
            raise ValueError("Fact text is required")
        conn.execute(
            """
            INSERT INTO fun_fact_schedule (target_date, fact_text, library_id, set_by)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(target_date) DO UPDATE SET
                fact_text = excluded.fact_text,
                library_id = excluded.library_id,
                set_by = excluded.set_by,
                created_at = datetime('now')
            """,
            (day, text, lib_id, by),
        )
        row = conn.execute(
            "SELECT id FROM fun_fact_schedule WHERE target_date = ?",
            (day,),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to save fun fact of the day")
        return int(row["id"])


def get_fun_fact_of_the_day(target_date: str | date | None = None) -> dict | None:
    d = _as_date(target_date or date.today())
    day = d.isoformat()
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, target_date, fact_text, library_id, set_by, created_at
            FROM fun_fact_schedule
            WHERE target_date = ?
            """,
            (day,),
        ).fetchone()
        if row:
            return dict(row)
        count_row = conn.execute(
            "SELECT COUNT(1) AS cnt FROM fun_fact_library WHERE active = 1",
        ).fetchone()
        total = int(count_row["cnt"]) if count_row else 0
        if total <= 0:
            return None
        idx = (d.timetuple().tm_yday - 1) % total
        fallback = conn.execute(
            """
            SELECT id, fact_text
            FROM fun_fact_library
            WHERE active = 1
            ORDER BY id ASC
            LIMIT 1 OFFSET ?
            """,
            (idx,),
        ).fetchone()
        if not fallback:
            return None
        return {
            "id": None,
            "target_date": day,
            "fact_text": str(fallback["fact_text"]),
            "library_id": int(fallback["id"]),
            "set_by": "system_default",
            "created_at": None,
        }


def total_completed_service_hours(child_id: int | None = None) -> float:
    query = """
        SELECT COALESCE(SUM(hours), 0) AS total
        FROM service_entries
        WHERE status = 'completed'
    """
    params: tuple[object, ...] = ()
    if child_id is not None:
        query += " AND child_id = ?"
        params = (child_id,)
    with get_connection() as conn:
        row = conn.execute(query, params).fetchone()
        return float(row["total"]) if row else 0.0


def review_service_hours(
    entry_id: int,
    decision: str,
    reviewed_by: str,
    review_note: str = "",
) -> bool:
    if decision not in ("completed", "rejected"):
        raise ValueError("decision must be completed or rejected")
    reviewer = reviewed_by.strip()
    if not reviewer:
        raise ValueError("Parent name is required")
    rates = get_service_credit_rates()

    with get_connection() as conn:
        entry = conn.execute(
            """
            SELECT id, child_id, hours, status
            FROM service_entries
            WHERE id = ?
            """,
            (entry_id,),
        ).fetchone()
        if not entry:
            raise ValueError("Service entry not found")
        if entry["status"] != "pending_parent":
            return False

        cursor = conn.execute(
            """
            UPDATE service_entries
            SET status = ?,
                reviewed_by = ?,
                review_note = ?,
                reviewed_at = datetime('now')
            WHERE id = ? AND status = 'pending_parent'
            """,
            (decision, reviewer, review_note.strip(), entry_id),
        )
        if cursor.rowcount == 0:
            return False

        if decision == "completed":
            hours = float(entry["hours"])
            allowance_credit = round(hours * rates["allowance_per_hour"], 2)
            screen_credit = round(hours * rates["screen_minutes_per_hour"], 2)
            if allowance_credit > 0:
                conn.execute(
                    """
                    INSERT INTO ledger_entries (
                        child_id,
                        asset_type,
                        amount,
                        source_type,
                        note
                    )
                    VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
                    """,
                    (entry["child_id"], allowance_credit, f"Service hours credit (entry #{entry_id})"),
                )
            if screen_credit > 0:
                conn.execute(
                    """
                    INSERT INTO ledger_entries (
                        child_id,
                        asset_type,
                        amount,
                        source_type,
                        note
                    )
                    VALUES (?, 'screen_time', ?, 'manual_adjustment', ?)
                    """,
                    (entry["child_id"], screen_credit, f"Service hours credit (entry #{entry_id})"),
                )
    return True


def add_task(name: str, rank: str, payout_type: str, payout_value: float = 0.0) -> int:
    cleaned_name = name.strip()
    value = round(float(payout_value), 2)
    if not cleaned_name:
        raise ValueError("Task name is required")
    if value < 0:
        raise ValueError("Task payout value must be >= 0")
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO tasks (name, rank, payout_type, payout_value)
            VALUES (?, ?, ?, ?)
            """,
            (cleaned_name, rank, payout_type, value),
        )
        return int(cursor.lastrowid)


def list_tasks(active_only: bool = True) -> Iterable[dict]:
    query = "SELECT id, name, rank, payout_type, payout_value, active, created_at FROM tasks"
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY rank ASC, name ASC"

    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def add_reward(name: str, reward_type: str, cost: float) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO rewards (name, reward_type, cost)
            VALUES (?, ?, ?)
            """,
            (name.strip(), reward_type, cost),
        )
        return int(cursor.lastrowid)


def list_rewards(active_only: bool = True) -> Iterable[dict]:
    query = "SELECT id, name, reward_type, cost, active, created_at FROM rewards"
    if active_only:
        query += " WHERE active = 1"
    query += " ORDER BY reward_type ASC, cost ASC"

    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def record_task_completion(
    child_id: int,
    task_id: int,
    completion_note: str = "",
    task_instance_id: int | None = None,
) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO task_completions (
                child_id,
                task_id,
                payout_type,
                payout_value,
                completion_note,
                task_instance_id
            )
            SELECT ?, t.id, t.payout_type, t.payout_value, ?, ?
            FROM tasks t
            WHERE t.id = ? AND t.active = 1
            """,
            (child_id, completion_note.strip(), task_instance_id, task_id),
        )
        if cursor.rowcount == 0:
            raise ValueError("Task not found or inactive")
        return int(cursor.lastrowid)


def _award_weekly_allowance_if_earned_conn(conn: sqlite3.Connection, completion_id: int) -> None:
    row = conn.execute(
        """
        SELECT
            tc.child_id,
            ti.due_date,
            ts.plan_scope
        FROM task_completions tc
        JOIN task_instances ti ON ti.id = tc.task_instance_id
        JOIN task_schedules ts ON ts.id = ti.schedule_id
        WHERE tc.id = ?
        """,
        (completion_id,),
    ).fetchone()
    if not row:
        return
    plan_scope = str(row["plan_scope"] or "standard")
    if plan_scope not in (WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
        return
    week_key = current_week_key(_as_date(str(row["due_date"])))
    child_id = int(row["child_id"])
    status = _get_weekly_allowance_progress_conn(conn, child_id, week_key)
    if status["credited"] or status["total_planned"] <= 0 or status["approved_count"] < status["total_planned"]:
        return
    amount = round(float(status["allowance_amount"]), 2)
    if amount <= 0:
        return
    cursor = conn.execute(
        """
        INSERT INTO ledger_entries (
            child_id,
            asset_type,
            amount,
            source_type,
            note
        )
        VALUES (?, 'allowance', ?, 'manual_adjustment', ?)
        """,
        (child_id, amount, f"Weekly allowance credit for {week_key}"),
    )
    conn.execute(
        """
        INSERT INTO weekly_allowance_credits (child_id, week_key, allowance_amount, ledger_entry_id)
        VALUES (?, ?, ?, ?)
        """,
        (child_id, week_key, amount, int(cursor.lastrowid)),
    )


def review_task_completion(
    completion_id: int,
    decision: str,
    reviewed_by: str,
    review_note: str = "",
) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE task_completions
            SET status = ?,
                reviewed_by = ?,
                review_note = ?,
                reviewed_at = datetime('now')
            WHERE id = ?
              AND status = 'pending'
            """,
            (decision, reviewed_by.strip(), review_note.strip(), completion_id),
        )
        if cursor.rowcount == 0:
            return False

        if decision == "approved":
            schedule_row = conn.execute(
                """
                SELECT ts.plan_scope
                FROM task_completions tc
                LEFT JOIN task_instances ti ON ti.id = tc.task_instance_id
                LEFT JOIN task_schedules ts ON ts.id = ti.schedule_id
                WHERE tc.id = ?
                """,
                (completion_id,),
            ).fetchone()
            plan_scope = str(schedule_row["plan_scope"] or "standard") if schedule_row else "standard"
            if plan_scope not in (WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
                conn.execute(
                    """
                    INSERT INTO ledger_entries (
                        child_id,
                        asset_type,
                        amount,
                        source_type,
                        task_completion_id,
                        note
                    )
                    SELECT
                        child_id,
                        payout_type,
                        payout_value,
                        'task_completion',
                        id,
                        ?
                    FROM task_completions
                    WHERE id = ?
                    """,
                    ("Earned from approved task completion", completion_id),
                )
            conn.execute(
                """
                UPDATE task_instances
                SET status = 'approved'
                WHERE task_completion_id = ?
                """,
                (completion_id,),
            )
            _award_weekly_allowance_if_earned_conn(conn, completion_id)
        elif decision == "rejected":
            conn.execute(
                """
                UPDATE task_instances
                SET status = 'rejected'
                WHERE task_completion_id = ?
                """,
                (completion_id,),
            )
        return True


def list_task_completions(status: str | None = None, child_id: int | None = None) -> Iterable[dict]:
    query = """
        SELECT
            tc.id,
            tc.status,
            c.name AS child_name,
            c.age AS child_age,
            t.name AS task_name,
            t.rank AS task_rank,
            tc.payout_type,
            tc.payout_value,
            tc.completion_note,
            ti.due_date,
            tc.completed_at,
            tc.reviewed_by,
            tc.review_note,
            tc.reviewed_at
        FROM task_completions tc
        JOIN children c ON c.id = tc.child_id
        JOIN tasks t ON t.id = tc.task_id
        LEFT JOIN task_instances ti ON ti.id = tc.task_instance_id
    """
    where_clauses: list[str] = []
    params: list[object] = []

    if status:
        where_clauses.append("tc.status = ?")
        params.append(status)
    if child_id is not None:
        where_clauses.append("tc.child_id = ?")
        params.append(child_id)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY tc.completed_at DESC, tc.id DESC"

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def add_task_schedule(
    task_id: int,
    cadence: str,
    child_id: int | None = None,
    day_of_week: int | None = None,
    period_mode: str = WEEKLY_PERIOD_DAY_OF_WEEK,
    times_per_period: int = 1,
    due_time: str | None = None,
    plan_scope: str = "standard",
    week_key: str | None = None,
) -> int:
    scope = (plan_scope or "standard").strip()
    if scope not in ("standard", WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
        raise ValueError("Invalid schedule scope")
    normalized_period_mode = _normalize_weekly_period_mode(period_mode)
    normalized_times_per_period = _normalize_times_per_period(times_per_period)
    if cadence == "daily":
        day_of_week = None
        normalized_period_mode = WEEKLY_PERIOD_DAY_OF_WEEK
        normalized_times_per_period = 1
    elif cadence == "weekly":
        if scope == "standard":
            normalized_period_mode = WEEKLY_PERIOD_DAY_OF_WEEK
            normalized_times_per_period = 1
        if normalized_period_mode == WEEKLY_PERIOD_DAY_OF_WEEK:
            if day_of_week is None:
                raise ValueError("Weekly schedules require day_of_week (0=Mon..6=Sun)")
        else:
            if scope not in (WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
                raise ValueError("Only weekly allowance plans support all-days or times-per-period rules")
            day_of_week = None
            if normalized_period_mode != WEEKLY_PERIOD_TIMES_PER_PERIOD:
                normalized_times_per_period = 1
    if due_time:
        due_time = _as_hhmm(due_time)
    normalized_week_key = None
    if scope == WEEKLY_ALLOWANCE_DEFAULT_SCOPE:
        if child_id is None:
            raise ValueError("Weekly allowance default schedules require child_id")
        if cadence != "weekly":
            raise ValueError("Weekly allowance default schedules must use weekly cadence")
    elif scope == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE:
        if child_id is None:
            raise ValueError("Weekly allowance override schedules require child_id")
        if cadence != "weekly":
            raise ValueError("Weekly allowance override schedules must use weekly cadence")
        if not week_key:
            raise ValueError("Weekly allowance override schedules require week_key")
        normalized_week_key = _normalize_week_key(week_key)
    elif week_key:
        normalized_week_key = _normalize_week_key(week_key)
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO task_schedules (
                task_id,
                child_id,
                cadence,
                day_of_week,
                period_mode,
                times_per_period,
                due_time,
                plan_scope,
                week_key
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                child_id,
                cadence,
                day_of_week,
                normalized_period_mode,
                normalized_times_per_period,
                due_time,
                scope,
                normalized_week_key,
            ),
        )
        return int(cursor.lastrowid)


def list_task_schedules(active_only: bool = True) -> Iterable[dict]:
    query = """
        SELECT
            ts.id,
            ts.task_id,
            t.name AS task_name,
            ts.child_id,
            c.name AS child_name,
            ts.cadence,
            ts.day_of_week,
            ts.period_mode,
            ts.times_per_period,
            ts.due_time,
            ts.plan_scope,
            ts.week_key,
            ts.active
        FROM task_schedules ts
        JOIN tasks t ON t.id = ts.task_id
        LEFT JOIN children c ON c.id = ts.child_id
    """
    query += " WHERE ts.plan_scope = 'standard'"
    if active_only:
        query += " AND ts.active = 1"
    query += " ORDER BY ts.id ASC"
    with get_connection() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def update_task_schedule(
    schedule_id: int,
    task_id: int,
    cadence: str,
    child_id: int | None = None,
    day_of_week: int | None = None,
    period_mode: str = WEEKLY_PERIOD_DAY_OF_WEEK,
    times_per_period: int = 1,
    due_time: str | None = None,
    plan_scope: str = "standard",
    week_key: str | None = None,
) -> bool:
    scope = (plan_scope or "standard").strip()
    if scope not in ("standard", WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
        raise ValueError("Invalid schedule scope")
    normalized_period_mode = _normalize_weekly_period_mode(period_mode)
    normalized_times_per_period = _normalize_times_per_period(times_per_period)
    if cadence == "daily":
        day_of_week = None
        normalized_period_mode = WEEKLY_PERIOD_DAY_OF_WEEK
        normalized_times_per_period = 1
    elif cadence == "weekly":
        if scope == "standard":
            normalized_period_mode = WEEKLY_PERIOD_DAY_OF_WEEK
            normalized_times_per_period = 1
        if normalized_period_mode == WEEKLY_PERIOD_DAY_OF_WEEK:
            if day_of_week is None:
                raise ValueError("Weekly schedules require day_of_week (0=Mon..6=Sun)")
        else:
            if scope not in (WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE):
                raise ValueError("Only weekly allowance plans support all-days or times-per-period rules")
            day_of_week = None
            if normalized_period_mode != WEEKLY_PERIOD_TIMES_PER_PERIOD:
                normalized_times_per_period = 1
    if due_time:
        due_time = _as_hhmm(due_time)
    normalized_week_key = None
    if scope == WEEKLY_ALLOWANCE_DEFAULT_SCOPE:
        if child_id is None:
            raise ValueError("Weekly allowance default schedules require child_id")
        if cadence != "weekly":
            raise ValueError("Weekly allowance default schedules must use weekly cadence")
    elif scope == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE:
        if child_id is None:
            raise ValueError("Weekly allowance override schedules require child_id")
        if cadence != "weekly":
            raise ValueError("Weekly allowance override schedules must use weekly cadence")
        if not week_key:
            raise ValueError("Weekly allowance override schedules require week_key")
        normalized_week_key = _normalize_week_key(week_key)
    elif week_key:
        normalized_week_key = _normalize_week_key(week_key)
    with get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE task_schedules
            SET task_id = ?,
                child_id = ?,
                cadence = ?,
                day_of_week = ?,
                period_mode = ?,
                times_per_period = ?,
                due_time = ?,
                plan_scope = ?,
                week_key = ?
            WHERE id = ?
            """,
            (
                task_id,
                child_id,
                cadence,
                day_of_week,
                normalized_period_mode,
                normalized_times_per_period,
                due_time,
                scope,
                normalized_week_key,
                schedule_id,
            ),
        )
        return cursor.rowcount > 0


def set_task_schedule_active(schedule_id: int, active: bool) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "UPDATE task_schedules SET active = ? WHERE id = ?",
            (1 if active else 0, schedule_id),
        )
        return cursor.rowcount > 0


def delete_task_schedule(schedule_id: int) -> bool:
    with get_connection() as conn:
        linked = conn.execute(
            "SELECT COUNT(1) AS cnt FROM task_instances WHERE schedule_id = ?",
            (schedule_id,),
        ).fetchone()
        linked_count = int(linked["cnt"]) if linked else 0
        if linked_count > 0:
            raise ValueError(f"Cannot delete schedule with {linked_count} linked task instances")
        cursor = conn.execute("DELETE FROM task_schedules WHERE id = ?", (schedule_id,))
        return cursor.rowcount > 0


def get_weekly_allowance_default_amount(child_id: int) -> float:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT default_amount FROM weekly_allowance_settings WHERE child_id = ?",
            (child_id,),
        ).fetchone()
        return round(float(row["default_amount"]) if row else 0.0, 2)


def set_weekly_allowance_default_amount(child_id: int, amount: float) -> bool:
    value = round(float(amount), 2)
    if value < 0:
        raise ValueError("Weekly allowance amount must be >= 0")
    with get_connection() as conn:
        exists = conn.execute("SELECT 1 FROM children WHERE id = ?", (child_id,)).fetchone()
        if not exists:
            return False
        conn.execute(
            """
            INSERT INTO weekly_allowance_settings (child_id, default_amount, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(child_id) DO UPDATE SET
                default_amount = excluded.default_amount,
                updated_at = datetime('now')
            """,
            (child_id, value),
        )
        return True


def get_weekly_allowance_override_amount(child_id: int, week_key: str) -> float | None:
    normalized_week_key = _normalize_week_key(week_key)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT allowance_amount
            FROM weekly_allowance_overrides
            WHERE child_id = ? AND week_key = ?
            """,
            (child_id, normalized_week_key),
        ).fetchone()
        if not row:
            return None
        return round(float(row["allowance_amount"]), 2)


def set_weekly_allowance_override_amount(child_id: int, week_key: str, amount: float) -> bool:
    value = round(float(amount), 2)
    if value < 0:
        raise ValueError("Weekly allowance amount must be >= 0")
    normalized_week_key = _normalize_week_key(week_key)
    with get_connection() as conn:
        exists = conn.execute("SELECT 1 FROM children WHERE id = ?", (child_id,)).fetchone()
        if not exists:
            return False
        conn.execute(
            """
            INSERT INTO weekly_allowance_overrides (child_id, week_key, allowance_amount)
            VALUES (?, ?, ?)
            ON CONFLICT(child_id, week_key) DO UPDATE SET
                allowance_amount = excluded.allowance_amount
            """,
            (child_id, normalized_week_key, value),
        )
        return True


def add_weekly_allowance_plan_item(
    child_id: int,
    task_id: int,
    day_of_week: int | None = None,
    period_mode: str = WEEKLY_PERIOD_DAY_OF_WEEK,
    times_per_period: int = 1,
    due_time: str | None = None,
    week_key: str | None = None,
) -> int:
    with get_connection() as conn:
        task = conn.execute(
            "SELECT id, rank FROM tasks WHERE id = ? AND active = 1",
            (task_id,),
        ).fetchone()
        if not task:
            raise ValueError("Task not found or inactive")
        if str(task["rank"]) != "required":
            raise ValueError("Weekly allowance plan items must use required tasks")
    scope = WEEKLY_ALLOWANCE_OVERRIDE_SCOPE if week_key else WEEKLY_ALLOWANCE_DEFAULT_SCOPE
    return add_task_schedule(
        task_id=task_id,
        cadence="weekly",
        child_id=child_id,
        day_of_week=day_of_week,
        period_mode=period_mode,
        times_per_period=times_per_period,
        due_time=due_time,
        plan_scope=scope,
        week_key=week_key,
    )


def delete_weekly_allowance_plan_item(schedule_id: int) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT plan_scope FROM task_schedules WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        if not row:
            return False
        if str(row["plan_scope"] or "standard") not in (
            WEEKLY_ALLOWANCE_DEFAULT_SCOPE,
            WEEKLY_ALLOWANCE_OVERRIDE_SCOPE,
        ):
            raise ValueError("Schedule is not a weekly allowance plan item")
    return delete_task_schedule(schedule_id)


def list_weekly_allowance_plan_items(
    child_id: int | None = None,
    week_key: str | None = None,
    include_inactive: bool = False,
) -> Iterable[dict]:
    query = """
        SELECT
            ts.id,
            ts.task_id,
            t.name AS task_name,
            ts.child_id,
            c.name AS child_name,
            ts.day_of_week,
            ts.period_mode,
            ts.times_per_period,
            ts.due_time,
            ts.plan_scope,
            ts.week_key,
            ts.active
        FROM task_schedules ts
        JOIN tasks t ON t.id = ts.task_id
        JOIN children c ON c.id = ts.child_id
        WHERE ts.plan_scope IN (?, ?)
    """
    params: list[object] = [WEEKLY_ALLOWANCE_DEFAULT_SCOPE, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE]
    if child_id is not None:
        query += " AND ts.child_id = ?"
        params.append(child_id)
    if week_key is not None:
        normalized_week_key = _normalize_week_key(week_key)
        query += " AND ((ts.plan_scope = ? AND ts.week_key = ?) OR ts.plan_scope = ?)"
        params.extend([WEEKLY_ALLOWANCE_OVERRIDE_SCOPE, normalized_week_key, WEEKLY_ALLOWANCE_DEFAULT_SCOPE])
    if not include_inactive:
        query += " AND ts.active = 1"
    query += (
        " ORDER BY c.name ASC, ts.plan_scope ASC, COALESCE(ts.week_key, ''), "
        "CASE ts.period_mode "
        f"WHEN '{WEEKLY_PERIOD_ALL_DAYS}' THEN 0 "
        f"WHEN '{WEEKLY_PERIOD_TIMES_PER_PERIOD}' THEN 1 "
        "ELSE 2 END ASC, "
        "COALESCE(ts.day_of_week, 99) ASC, COALESCE(ts.due_time, '') ASC, t.name ASC"
    )
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def clone_weekly_allowance_override_from_default(child_id: int, week_key: str) -> int:
    normalized_week_key = _normalize_week_key(week_key)
    copied = 0
    with get_connection() as conn:
        exists = conn.execute("SELECT 1 FROM children WHERE id = ?", (child_id,)).fetchone()
        if not exists:
            raise ValueError("Child not found")
        default_amount = get_weekly_allowance_default_amount(child_id)
        conn.execute(
            """
            INSERT INTO weekly_allowance_overrides (child_id, week_key, allowance_amount)
            VALUES (?, ?, ?)
            ON CONFLICT(child_id, week_key) DO NOTHING
            """,
            (child_id, normalized_week_key, default_amount),
        )
        default_rows = conn.execute(
            """
            SELECT task_id, day_of_week, period_mode, times_per_period, due_time
            FROM task_schedules
            WHERE child_id = ?
              AND plan_scope = ?
              AND active = 1
            ORDER BY COALESCE(day_of_week, 99) ASC, COALESCE(due_time, '') ASC, id ASC
            """,
            (child_id, WEEKLY_ALLOWANCE_DEFAULT_SCOPE),
        ).fetchall()
        existing = {
            (
                int(row["task_id"]),
                row["day_of_week"],
                str(row["period_mode"] or WEEKLY_PERIOD_DAY_OF_WEEK),
                int(row["times_per_period"] or 1),
                str(row["due_time"] or ""),
            )
            for row in conn.execute(
                """
                SELECT task_id, day_of_week, period_mode, times_per_period, due_time
                FROM task_schedules
                WHERE child_id = ?
                  AND plan_scope = ?
                  AND week_key = ?
                  AND active = 1
                """,
                (child_id, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE, normalized_week_key),
            ).fetchall()
        }
        for row in default_rows:
            signature = (
                int(row["task_id"]),
                row["day_of_week"],
                str(row["period_mode"] or WEEKLY_PERIOD_DAY_OF_WEEK),
                int(row["times_per_period"] or 1),
                str(row["due_time"] or ""),
            )
            if signature in existing:
                continue
            conn.execute(
                """
                INSERT INTO task_schedules (
                    task_id,
                    child_id,
                    cadence,
                    day_of_week,
                    period_mode,
                    times_per_period,
                    due_time,
                    plan_scope,
                    week_key
                )
                VALUES (?, ?, 'weekly', ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(row["task_id"]),
                    child_id,
                    row["day_of_week"],
                    str(row["period_mode"] or WEEKLY_PERIOD_DAY_OF_WEEK),
                    int(row["times_per_period"] or 1),
                    row["due_time"],
                    WEEKLY_ALLOWANCE_OVERRIDE_SCOPE,
                    normalized_week_key,
                ),
            )
            copied += 1
            existing.add(signature)
    return copied


def _weekly_allowance_override_exists(conn: sqlite3.Connection, child_id: int, week_key: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM task_schedules
        WHERE child_id = ?
          AND plan_scope = ?
          AND week_key = ?
          AND active = 1
        LIMIT 1
        """,
        (child_id, WEEKLY_ALLOWANCE_OVERRIDE_SCOPE, week_key),
    ).fetchone()
    return row is not None


def _get_effective_weekly_allowance_amount_conn(conn: sqlite3.Connection, child_id: int, week_key: str) -> float:
    row = conn.execute(
        """
        SELECT allowance_amount
        FROM weekly_allowance_overrides
        WHERE child_id = ? AND week_key = ?
        """,
        (child_id, week_key),
    ).fetchone()
    if row:
        return round(float(row["allowance_amount"]), 2)
    row = conn.execute(
        "SELECT default_amount FROM weekly_allowance_settings WHERE child_id = ?",
        (child_id,),
    ).fetchone()
    return round(float(row["default_amount"]) if row else 0.0, 2)


def _get_effective_weekly_allowance_scope_conn(conn: sqlite3.Connection, child_id: int, week_key: str) -> str:
    if _weekly_allowance_override_exists(conn, child_id, week_key):
        return WEEKLY_ALLOWANCE_OVERRIDE_SCOPE
    return WEEKLY_ALLOWANCE_DEFAULT_SCOPE


def _get_weekly_allowance_progress_conn(
    conn: sqlite3.Connection,
    child_id: int,
    week_key: str,
) -> dict:
    start, end = _week_bounds(week_key)
    scope = _get_effective_weekly_allowance_scope_conn(conn, child_id, week_key)
    params: list[object] = [child_id, start.isoformat(), end.isoformat(), scope]
    week_clause = ""
    if scope == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE:
        week_clause = " AND ts.week_key = ?"
        params.append(week_key)
    total_row = conn.execute(
        f"""
        SELECT COUNT(1) AS cnt
        FROM task_instances ti
        JOIN task_schedules ts ON ts.id = ti.schedule_id
        WHERE ti.child_id = ?
          AND date(ti.due_date) BETWEEN date(?) AND date(?)
          AND ts.plan_scope = ?
          {week_clause}
        """,
        tuple(params),
    ).fetchone()
    approved_row = conn.execute(
        f"""
        SELECT COUNT(1) AS cnt
        FROM task_instances ti
        JOIN task_schedules ts ON ts.id = ti.schedule_id
        WHERE ti.child_id = ?
          AND date(ti.due_date) BETWEEN date(?) AND date(?)
          AND ts.plan_scope = ?
          {week_clause}
          AND ti.status = 'approved'
        """,
        tuple(params),
    ).fetchone()
    credited_row = conn.execute(
        """
        SELECT id, ledger_entry_id
        FROM weekly_allowance_credits
        WHERE child_id = ? AND week_key = ?
        """,
        (child_id, week_key),
    ).fetchone()
    total_planned = int(total_row["cnt"]) if total_row else 0
    approved_count = int(approved_row["cnt"]) if approved_row else 0
    return {
        "week_key": week_key,
        "plan_source": "override" if scope == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE else "default",
        "total_planned": total_planned,
        "approved_count": approved_count,
        "allowance_amount": _get_effective_weekly_allowance_amount_conn(conn, child_id, week_key),
        "credited": credited_row is not None,
        "credit_id": int(credited_row["id"]) if credited_row else None,
        "credit_ledger_entry_id": int(credited_row["ledger_entry_id"]) if credited_row and credited_row["ledger_entry_id"] else None,
    }


def get_weekly_allowance_status(child_id: int, week_key: str | None = None) -> dict:
    normalized_week_key = current_week_key() if week_key is None else _normalize_week_key(week_key)
    with get_connection() as conn:
        status = _get_weekly_allowance_progress_conn(conn, child_id, normalized_week_key)
        status["default_amount"] = get_weekly_allowance_default_amount(child_id)
        override_amount = get_weekly_allowance_override_amount(child_id, normalized_week_key)
        status["override_amount"] = override_amount
        return status


def generate_task_instances(start_on: str | date, end_on: str | date) -> int:
    start_date = _as_date(start_on)
    end_date = _as_date(end_on)
    if end_date < start_date:
        raise ValueError("end_on must be >= start_on")

    created = 0
    with get_connection() as conn:
        schedules = conn.execute(
            """
            SELECT
                id,
                task_id,
                child_id,
                cadence,
                day_of_week,
                period_mode,
                times_per_period,
                due_time,
                plan_scope,
                week_key
            FROM task_schedules
            WHERE active = 1
            """
        ).fetchall()
        active_children = conn.execute("SELECT id FROM children WHERE active = 1").fetchall()
        child_ids = [int(row["id"]) for row in active_children]
        override_weeks = {
            (int(schedule["child_id"]), str(schedule["week_key"]))
            for schedule in schedules
            if str(schedule["plan_scope"] or "standard") == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE
            and schedule["child_id"] is not None
            and schedule["week_key"]
        }

        current = start_date
        while current <= end_date:
            weekday = current.weekday()
            due_date = current.isoformat()
            week_key = current_week_key(current)
            for schedule in schedules:
                cadence = str(schedule["cadence"])
                if cadence not in ("daily", "weekly"):
                    continue
                if cadence == "weekly":
                    period_mode = str(schedule["period_mode"] or WEEKLY_PERIOD_DAY_OF_WEEK)
                    if period_mode == WEEKLY_PERIOD_DAY_OF_WEEK:
                        if int(schedule["day_of_week"]) != weekday:
                            continue
                    elif period_mode == WEEKLY_PERIOD_ALL_DAYS:
                        pass
                    elif period_mode == WEEKLY_PERIOD_TIMES_PER_PERIOD:
                        if weekday >= int(schedule["times_per_period"] or 1):
                            continue
                    else:
                        continue
                plan_scope = str(schedule["plan_scope"] or "standard")
                if plan_scope == WEEKLY_ALLOWANCE_OVERRIDE_SCOPE:
                    if str(schedule["week_key"] or "") != week_key:
                        continue
                elif plan_scope == WEEKLY_ALLOWANCE_DEFAULT_SCOPE:
                    if schedule["child_id"] is None:
                        continue
                    if (int(schedule["child_id"]), week_key) in override_weeks:
                        continue
                targets = [int(schedule["child_id"])] if schedule["child_id"] is not None else child_ids
                for cid in targets:
                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO task_instances (
                            schedule_id, task_id, child_id, due_date, due_time
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (schedule["id"], schedule["task_id"], cid, due_date, schedule["due_time"]),
                    )
                    if cursor.rowcount > 0:
                        created += 1
            current += timedelta(days=1)
    return created


def list_due_task_instances(
    child_id: int | None = None,
    due_date: str | date | None = None,
    include_statuses: tuple[str, ...] = ("open", "rejected", "submitted"),
) -> Iterable[dict]:
    query = """
        SELECT
            ti.id,
            ti.task_id,
            t.name AS task_name,
            t.rank AS task_rank,
            t.payout_type,
            t.payout_value,
            ti.child_id,
            c.name AS child_name,
            ti.due_date,
            ti.due_time,
            ti.status,
            ti.task_completion_id
        FROM task_instances ti
        JOIN tasks t ON t.id = ti.task_id
        JOIN children c ON c.id = ti.child_id
    """
    params: list[object] = []
    where: list[str] = []
    if include_statuses:
        placeholders = ",".join(["?"] * len(include_statuses))
        where.append(f"ti.status IN ({placeholders})")
        params.extend(include_statuses)
    if child_id is not None:
        where.append("ti.child_id = ?")
        params.append(child_id)
    if due_date is not None:
        where.append("ti.due_date = ?")
        params.append(_as_date(due_date).isoformat())
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY ti.due_date ASC, ti.due_time ASC, t.rank ASC, t.name ASC"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def submit_task_instance(instance_id: int, completion_note: str = "") -> int:
    with get_connection() as conn:
        instance = conn.execute(
            """
            SELECT id, task_id, child_id, status
            FROM task_instances
            WHERE id = ?
            """,
            (instance_id,),
        ).fetchone()
        if not instance:
            raise ValueError("Task instance not found")
        if instance["status"] not in ("open", "rejected"):
            raise ValueError("Task instance is not available for submission")

        cursor = conn.execute(
            """
            INSERT INTO task_completions (
                child_id,
                task_id,
                payout_type,
                payout_value,
                completion_note,
                task_instance_id
            )
            SELECT ?, t.id, t.payout_type, t.payout_value, ?, ?
            FROM tasks t
            WHERE t.id = ? AND t.active = 1
            """,
            (
                int(instance["child_id"]),
                completion_note.strip(),
                int(instance_id),
                int(instance["task_id"]),
            ),
        )
        if cursor.rowcount == 0:
            raise ValueError("Task not found or inactive")
        completion_id = int(cursor.lastrowid)
        conn.execute(
            """
            UPDATE task_instances
            SET status = 'submitted',
                task_completion_id = ?
            WHERE id = ?
            """,
            (completion_id, instance_id),
        )
        return completion_id


def add_manual_ledger_entry(
    child_id: int,
    asset_type: str,
    amount: float,
    note: str = "",
) -> int:
    if amount == 0:
        raise ValueError("Amount cannot be 0")

    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ledger_entries (
                child_id,
                asset_type,
                amount,
                source_type,
                note
            )
            VALUES (?, ?, ?, 'manual_adjustment', ?)
            """,
            (child_id, asset_type, amount, note.strip()),
        )
        return int(cursor.lastrowid)


def redeem_reward(child_id: int, reward_id: int, note: str = "") -> int:
    with get_connection() as conn:
        reward = conn.execute(
            """
            SELECT id, name, reward_type, cost
            FROM rewards
            WHERE id = ? AND active = 1
            """,
            (reward_id,),
        ).fetchone()
        if not reward:
            raise ValueError("Reward not found or inactive")

        balance_row = conn.execute(
            """
            SELECT COALESCE(SUM(amount), 0) AS balance
            FROM ledger_entries
            WHERE child_id = ? AND asset_type = ?
            """,
            (child_id, reward["reward_type"]),
        ).fetchone()
        balance = float(balance_row["balance"]) if balance_row else 0.0
        cost = float(reward["cost"])
        if balance < cost:
            raise ValueError(
                f"Insufficient {reward['reward_type']} balance: has {balance}, needs {cost}"
            )

        cursor = conn.execute(
            """
            INSERT INTO ledger_entries (
                child_id,
                asset_type,
                amount,
                source_type,
                reward_id,
                note
            )
            VALUES (?, ?, ?, 'reward_redemption', ?, ?)
            """,
            (child_id, reward["reward_type"], -cost, reward_id, note.strip() or f"Redeemed: {reward['name']}"),
        )
        return int(cursor.lastrowid)


def list_balances(child_id: int | None = None) -> Iterable[dict]:
    query = """
        SELECT
            c.id AS child_id,
            c.name AS child_name,
            le.asset_type,
            ROUND(COALESCE(SUM(le.amount), 0), 2) AS balance
        FROM children c
        LEFT JOIN ledger_entries le ON le.child_id = c.id
        WHERE c.active = 1
    """
    params: list[object] = []
    if child_id is not None:
        query += " AND c.id = ?"
        params.append(child_id)
    query += """
        GROUP BY c.id, c.name, le.asset_type
        ORDER BY c.name ASC, le.asset_type ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows if row["asset_type"] is not None]


def list_ledger_entries(child_id: int | None = None) -> Iterable[dict]:
    query = """
        SELECT
            le.id,
            c.name AS child_name,
            le.asset_type,
            le.amount,
            le.source_type,
            t.name AS task_name,
            r.name AS reward_name,
            le.note,
            le.created_at
        FROM ledger_entries le
        JOIN children c ON c.id = le.child_id
        LEFT JOIN task_completions tc ON tc.id = le.task_completion_id
        LEFT JOIN tasks t ON t.id = tc.task_id
        LEFT JOIN rewards r ON r.id = le.reward_id
    """
    params: list[object] = []
    if child_id is not None:
        query += " WHERE le.child_id = ?"
        params.append(child_id)
    query += " ORDER BY le.created_at DESC, le.id DESC"

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]
