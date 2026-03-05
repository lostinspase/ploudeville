from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import hashlib
import hmac
import os
import re
import smtplib
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from html import escape
from typing import Callable
from urllib.parse import parse_qs, urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

from .db import init_db
from .repository import (
    add_manual_ledger_entry,
    add_message,
    add_charity,
    add_service_organization,
    adopt_weekly_pet,
    add_reward,
    add_task,
    add_task_schedule,
    apply_birthday_treatment,
    apply_parent_birthday_treatment,
    complete_pet_care,
    complete_donation_pledge,
    consume_child_pin_reset_token,
    consume_parent_reset_token,
    create_donation_pledge,
    create_pet_species,
    count_pet_adoptions,
    create_parent_reset_token,
    create_child_pin_reset_token,
    delete_task_schedule,
    generate_pet_help_messages,
    generate_task_instances,
    get_child,
    get_available_allowance_for_payout,
    get_pet_weekly_dashboard,
    get_parent_reset_emails,
    get_parent_reset_text_numbers,
    list_balances,
    apply_daily_wallet_interest,
    get_wallet_daily_interest_rate_percent,
    set_wallet_daily_interest_rate_percent,
    total_interest_earned,
    list_interest_accruals,
    list_children,
    list_due_task_instances,
    list_pet_adoptions,
    list_pet_badges,
    list_messages,
    list_charities,
    list_donation_pledges,
    list_wallet_payouts,
    create_reading_log,
    get_reading_log,
    list_reading_logs,
    set_reading_log_questions,
    update_reading_log_quiz_result,
    award_reading_log_credit,
    parent_override_reading_credit,
    total_completed_donations,
    list_pet_species,
    list_parents,
    list_today_birthdays,
    list_today_parent_birthdays,
    list_rewards,
    list_service_entries,
    list_service_organizations,
    list_task_completions,
    list_task_schedules,
    list_tasks,
    set_service_credit_rates,
    submit_service_hours,
    review_service_hours,
    total_completed_service_hours,
    redeem_reward,
    recheck_charity_website,
    request_wallet_payout,
    review_task_completion,
    mark_wallet_payout_sent,
    set_charity_tax_exempt_verified,
    set_task_schedule_active,
    set_default_pet,
    set_child_pin,
    update_child_contact_info,
    update_parent_contact_info,
    submit_task_instance,
    update_task_schedule,
    verify_parent_password,
    verify_child_pin,
    get_service_credit_rates,
    list_child_house_rules,
    get_after_school_reminders,
    set_after_school_reminders,
    upsert_child_house_rules,
    add_motd_library_message,
    get_message_of_the_day,
    list_motd_library,
    set_message_of_the_day,
    add_fun_fact_library_fact,
    get_fun_fact_of_the_day,
    list_fun_fact_library,
    set_fun_fact_of_the_day,
    add_child_activity,
    add_holiday,
    get_child_activities_for_date,
    list_child_activities,
    list_holidays,
    list_pending_activity_notifications,
    mark_activity_notification_sent,
    update_activity_notification_settings,
    get_screen_time_allotment_minutes,
    list_child_app_limits,
    upsert_child_app_limit,
    add_concert_goal,
    add_adventure_goal,
    add_adventure_park_catalog,
    list_concert_goals,
    list_child_concert_goal_progress,
    list_adventure_goals,
    list_child_adventure_goal_progress,
    list_adventure_park_catalog,
    search_ticketmaster_events,
    save_ios_usage_report,
    list_ios_usage_reports,
    list_ledger_entries,
    set_concert_goal_status,
    set_adventure_goal_status,
    list_activity_notification_attempts,
    log_activity_notification_attempt,
)

Environ = dict[str, object]
StartResponse = Callable[[str, list[tuple[str, str]]], None]
COOKIE_NAME = "kid_auth"
PARENT_COOKIE_NAME = "parent_auth"
COOKIE_SECRET = os.environ.get("FAMILY_WEB_SECRET", "family-local-dev-secret")


def _sign_child(child_id: int) -> str:
    raw = str(child_id).encode("utf-8")
    return hmac.new(COOKIE_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()[:24]


def _read_cookies(environ: Environ) -> dict[str, str]:
    raw = str(environ.get("HTTP_COOKIE") or "")
    parts = [segment.strip() for segment in raw.split(";") if segment.strip()]
    cookies: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies


def _is_child_authed(environ: Environ, child_id: int) -> bool:
    cookies = _read_cookies(environ)
    token = cookies.get(COOKIE_NAME)
    if not token or ":" not in token:
        return False
    token_child, token_sig = token.split(":", 1)
    if token_child != str(child_id):
        return False
    return hmac.compare_digest(token_sig, _sign_child(child_id))


def _sign_parent() -> str:
    return hmac.new(COOKIE_SECRET.encode("utf-8"), b"parent", hashlib.sha256).hexdigest()[:24]


def _is_parent_authed(environ: Environ) -> bool:
    cookies = _read_cookies(environ)
    token = cookies.get(PARENT_COOKIE_NAME, "")
    return hmac.compare_digest(token, _sign_parent())


def _send_parent_reset_email(to_email: str, token: str, host_url: str) -> None:
    reset_link = f"{host_url.rstrip('/')}/parent-reset?token={token}"
    _send_plain_email(
        to_email=to_email,
        subject="Ploudeville Family System Parent Password Reset",
        content=(
        "Use the link below to reset your parent password:\n"
        f"{reset_link}\n\n"
        "This link expires in 30 minutes."
        ),
    )


def _send_plain_email(to_email: str, subject: str, content: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST")
    if not smtp_host:
        raise ValueError("SMTP_HOST is not configured for email delivery")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user or "no-reply@ploudeville.local")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to_email
    msg.set_content(content)

    smtp_timeout = int(os.environ.get("SMTP_TIMEOUT_SECONDS", "8"))
    with smtplib.SMTP(smtp_host, smtp_port, timeout=smtp_timeout) as server:
        server.starttls()
        if smtp_user:
            server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def _send_parent_reset_emails(host_url: str, valid_minutes: int = 30) -> int:
    emails = get_parent_reset_emails()
    sent = 0
    for email in emails:
        token, to_email = create_parent_reset_token(valid_minutes=valid_minutes, email=email)
        _send_parent_reset_email(to_email or email, token, host_url)
        sent += 1
    return sent


def _send_parent_reset_texts(host_url: str, valid_minutes: int = 30) -> int:
    parents = list_parents(active_only=True)
    targets = [
        (str(p.get("email") or "").strip(), str(p.get("text_number") or "").strip())
        for p in parents
        if p.get("email") and p.get("text_number")
    ]
    if not targets:
        raise ValueError("No parent text numbers are configured")
    sent = 0
    for email, number in targets:
        token, _ = create_parent_reset_token(valid_minutes=valid_minutes, email=email)
        reset_link = f"{host_url.rstrip('/')}/parent-reset?token={token}"
        _send_apeiron_sms(number, f"Parent reset link: {reset_link}")
        sent += 1
    return sent


def _send_child_pin_reset_email(to_email: str, child_name: str, token: str, host_url: str) -> None:
    reset_link = f"{host_url.rstrip('/')}/child-pin-reset?token={token}"
    _send_plain_email(
        to_email=to_email,
        subject=f"PIN Reset for {child_name}",
        content=(
        f"Use this link to reset {child_name}'s PIN:\n"
        f"{reset_link}\n\n"
        "This link expires in 30 minutes."
        ),
    )


def _send_apeiron_sms(to_number: str, message_text: str) -> None:
    endpoint = os.environ.get("APEIRON_SMS_ENDPOINT", "https://api.apeiron.io/sms/send")
    api_key = os.environ.get("APEIRON_API_KEY", "")
    sms_user = os.environ.get("APEIRON_SMS_USER", "").strip()
    sms_token = os.environ.get("APEIRON_SMS_TOKEN", "").strip()
    sms_from = os.environ.get("APEIRON_SMS_FROM", "").strip()
    if not endpoint:
        raise ValueError("APEIRON_SMS_ENDPOINT is not configured")
    if not ((sms_user and sms_token) or api_key):
        raise ValueError("Set Apeiron credentials via APEIRON_SMS_USER+APEIRON_SMS_TOKEN or APEIRON_API_KEY")
    payload_obj = {"to": to_number, "message": message_text}
    if sms_from:
        payload_obj["from"] = sms_from
    payload = json.dumps(payload_obj).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if sms_user and sms_token:
        raw = f"{sms_user}:{sms_token}".encode("utf-8")
        headers["Authorization"] = f"Basic {base64.b64encode(raw).decode('ascii')}"
    else:
        headers["Authorization"] = f"Bearer {api_key}"
    req = Request(
        endpoint,
        data=payload,
        method="POST",
        headers=headers,
    )
    with urlopen(req, timeout=20):
        pass


def _fallback_reading_questions(book_title: str, chapters: str) -> tuple[str, str]:
    return (
        f"In your own words, what happened in {book_title} ({chapters})?",
        "What is one lesson, theme, or important idea from what you read?",
    )


def _chatbot_request(payload: dict) -> dict:
    endpoint = os.environ.get("READING_CHATBOT_API_URL", "").strip()
    if not endpoint:
        raise ValueError("READING_CHATBOT_API_URL is not configured")
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get("READING_CHATBOT_API_KEY", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = Request(
        endpoint,
        method="POST",
        headers=headers,
        data=json.dumps(payload).encode("utf-8"),
    )
    with urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body or "{}")


def _extract_json_object(raw: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("Empty response")
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON object found in model response")
    parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise ValueError("Model response JSON must be an object")
    return parsed


def _groq_chat_json(system_prompt: str, user_prompt: str) -> dict:
    api_key = os.environ.get("GROQ_API_KEY", "").strip()
    if not api_key:
        raise ValueError("GROQ_API_KEY is not configured")
    model = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant").strip() or "llama-3.1-8b-instant"
    endpoint = os.environ.get("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions").strip()
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }
    req = Request(
        endpoint,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        data=json.dumps(payload).encode("utf-8"),
    )
    with urlopen(req, timeout=25) as resp:
        body = resp.read().decode("utf-8")
    parsed = json.loads(body or "{}")
    choices = parsed.get("choices") or []
    if not isinstance(choices, list) or not choices:
        raise ValueError("Groq response did not include choices")
    content = str(((choices[0] or {}).get("message") or {}).get("content") or "")
    return _extract_json_object(content)


def _reading_adapter_response(payload: dict) -> dict:
    action = str(payload.get("action") or "").strip().lower()
    if action == "generate_questions":
        book_title = str(payload.get("book_title") or "").strip()
        chapters = str(payload.get("chapters") or "").strip()
        question_count_raw = payload.get("question_count", 2)
        try:
            question_count = int(question_count_raw)
        except Exception:
            question_count = 2
        question_count = 2 if question_count < 2 else min(question_count, 4)
        result = _groq_chat_json(
            system_prompt=(
                "You generate age-appropriate reading-comprehension questions for kids. "
                "Return strict JSON only."
            ),
            user_prompt=(
                f"Book title: {book_title}\n"
                f"Chapters/section: {chapters}\n"
                f"Generate exactly {question_count} short open-ended comprehension questions.\n"
                'Return JSON with key "questions" as an array of strings.'
            ),
        )
        questions = result.get("questions") or []
        if not isinstance(questions, list):
            raise ValueError("questions must be an array")
        questions = [str(q).strip() for q in questions if str(q).strip()]
        if len(questions) < 2:
            raise ValueError("Model returned fewer than 2 valid questions")
        return {"questions": questions[:question_count]}
    if action == "evaluate_answers":
        book_title = str(payload.get("book_title") or "").strip()
        chapters = str(payload.get("chapters") or "").strip()
        questions = payload.get("questions") or []
        answers = payload.get("answers") or []
        if not isinstance(questions, list) or not isinstance(answers, list):
            raise ValueError("questions and answers must be arrays")
        q1 = str(questions[0] if len(questions) > 0 else "").strip()
        q2 = str(questions[1] if len(questions) > 1 else "").strip()
        a1 = str(answers[0] if len(answers) > 0 else "").strip()
        a2 = str(answers[1] if len(answers) > 1 else "").strip()
        result = _groq_chat_json(
            system_prompt=(
                "You evaluate two reading-comprehension answers for a child. "
                "Pass if both answers are relevant and show understanding. "
                "Be lenient for young learners. Return strict JSON only."
            ),
            user_prompt=(
                f"Book title: {book_title}\n"
                f"Chapters/section: {chapters}\n"
                f"Question 1: {q1}\n"
                f"Answer 1: {a1}\n"
                f"Question 2: {q2}\n"
                f"Answer 2: {a2}\n"
                'Return JSON with keys: "passed" (boolean), "score" (0.0 to 1.0).'
            ),
        )
        passed = bool(result.get("passed"))
        score = max(0.0, min(float(result.get("score", 0.0)), 1.0))
        return {"passed": passed, "score": score}
    raise ValueError("Unsupported action")


def _generate_reading_quiz_questions(book_title: str, chapters: str) -> tuple[str, str, str]:
    payload = {
        "action": "generate_questions",
        "book_title": book_title,
        "chapters": chapters,
        "question_count": 2,
    }
    try:
        data = _chatbot_request(payload)
        questions = data.get("questions") or []
        if isinstance(questions, list) and len(questions) >= 2:
            q1 = str(questions[0]).strip()
            q2 = str(questions[1]).strip()
            if q1 and q2:
                return q1, q2, "chatbot_api"
    except Exception:
        pass
    q1, q2 = _fallback_reading_questions(book_title, chapters)
    return q1, q2, "fallback_local"


def _evaluate_reading_quiz_answers(
    book_title: str,
    chapters: str,
    question_1: str,
    question_2: str,
    answer_1: str,
    answer_2: str,
) -> tuple[bool, float]:
    payload = {
        "action": "evaluate_answers",
        "book_title": book_title,
        "chapters": chapters,
        "questions": [question_1, question_2],
        "answers": [answer_1, answer_2],
    }
    try:
        data = _chatbot_request(payload)
        passed = bool(data.get("passed"))
        score_raw = data.get("score", 0)
        score = float(score_raw)
        score = max(0.0, min(score, 1.0))
        return passed, score
    except Exception:
        # Local fallback: require both answers to be substantive.
        a1 = answer_1.strip()
        a2 = answer_2.strip()
        substantive = len(a1) >= 20 and len(a2) >= 20 and len(a1.split()) >= 4 and len(a2.split()) >= 4
        score = 1.0 if substantive else 0.0
        return substantive, score


def _process_activity_notifications() -> None:
    pending = list_pending_activity_notifications(datetime.now())
    for row in pending:
        message = (
            f"Reminder: {row['child_name']} has {row['activity_name']} at {row['start_time']} today."
        )
        if row["channel"] == "email":
            to_email = str(row.get("child_email") or "").strip()
            if not to_email:
                continue
            try:
                _send_plain_email(
                    to_email=to_email,
                    subject=f"Activity Reminder for {row['child_name']}",
                    content=message,
                )
                mark_activity_notification_sent(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="email",
                    sent_to=to_email,
                )
                log_activity_notification_attempt(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="email",
                    target=to_email,
                    success=True,
                )
            except Exception as err:  # noqa: BLE001
                log_activity_notification_attempt(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="email",
                    target=to_email,
                    success=False,
                    error_text=str(err),
                )
        elif row["channel"] == "sms":
            to_number = str(row.get("child_text_number") or "").strip()
            if not to_number:
                continue
            try:
                _send_apeiron_sms(to_number, message)
                mark_activity_notification_sent(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="sms",
                    sent_to=to_number,
                )
                log_activity_notification_attempt(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="sms",
                    target=to_number,
                    success=True,
                )
            except Exception as err:  # noqa: BLE001
                log_activity_notification_attempt(
                    activity_id=int(row["activity_id"]),
                    occurrence_date=str(row["occurrence_date"]),
                    channel="sms",
                    target=to_number,
                    success=False,
                    error_text=str(err),
                )


def _redirect(location: str, set_cookie: str | None = None) -> tuple[str, list[tuple[str, str]], bytes]:
    headers = [("Location", location), ("Content-Type", "text/plain; charset=utf-8")]
    if set_cookie:
        headers.append(("Set-Cookie", set_cookie))
    return ("303 See Other", headers, b"Redirecting...")


def _html_response(content: str, status: str = "200 OK") -> tuple[str, list[tuple[str, str]], bytes]:
    return (status, [("Content-Type", "text/html; charset=utf-8")], content.encode("utf-8"))


def _json_response(payload: dict, status: str = "200 OK") -> tuple[str, list[tuple[str, str]], bytes]:
    return (
        status,
        [("Content-Type", "application/json; charset=utf-8")],
        json.dumps(payload).encode("utf-8"),
    )


def _require_ios_bearer_if_configured(environ: Environ) -> bool:
    expected = os.environ.get("IOS_SYNC_TOKEN", "").strip()
    if not expected:
        return True
    auth_header = str(environ.get("HTTP_AUTHORIZATION") or "")
    return auth_header == f"Bearer {expected}"


def _as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _base_styles() -> str:
    return """
    :root {
      --bg0: #f6f2e8;
      --bg1: #fff9ef;
      --ink: #1f2a2e;
      --muted: #55666a;
      --brand: #126e61;
      --card: #fffefb;
      --radius: 16px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next", "Trebuchet MS", "Segoe UI", sans-serif;
      color: var(--ink);
      background: radial-gradient(circle at 20% -10%, #fff6db, transparent 45%),
                  linear-gradient(140deg, var(--bg0), var(--bg1));
    }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 24px 16px 48px; }
    header {
      background: linear-gradient(120deg, #11463f, #1a7265 72%, #e79f1a 130%);
      color: #fff; border-radius: var(--radius); padding: 16px; margin-bottom: 14px;
    }
    .nav { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }
    .nav a {
      color: #fff; text-decoration: none; border: 1px solid rgba(255,255,255,0.3);
      border-radius: 999px; padding: 6px 10px; font-weight: 700; font-size: 14px;
    }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
    .motd-row { display: grid; grid-template-columns: 1fr; gap: 12px; margin-bottom: 12px; }
    @media (min-width: 930px) { .motd-row { grid-template-columns: 1fr 1fr; } }
    .two-col { display: grid; grid-template-columns: 1fr; gap: 12px; }
    @media (min-width: 930px) { .two-col { grid-template-columns: 1.15fr 0.85fr; } }
    .card {
      background: var(--card); border: 1px solid #e7d9b8; border-radius: var(--radius);
      padding: 12px; box-shadow: 0 3px 9px rgba(33, 42, 40, 0.05);
    }
    table { width: 100%; border-collapse: collapse; background: #fffdf7; border: 1px solid #eadaae; border-radius: 12px; overflow: hidden; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #efe4c7; font-size: 14px; }
    th { background: #fff3d4; font-size: 13px; }
    tr:last-child td { border-bottom: none; }
    form { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    input, select, button {
      font: inherit; padding: 8px 10px; border: 1px solid #d9c899; border-radius: 10px;
      background: #fffef9; min-height: 34px;
    }
    button {
      border: none; background: linear-gradient(120deg, var(--brand), #1f8a7b);
      color: #fff; font-weight: 800; cursor: pointer;
    }
    .truncate {
      white-space: normal;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .muted { color: var(--muted); }
    .msg { border: 1px solid #b8e2c3; background: #ecf8ef; border-radius: 12px; padding: 10px; margin: 0 0 12px; }
    """


def _layout(title: str, body: str, message: str = "") -> str:
    msg_html = f'<p class="msg">{escape(message)}</p>' if message else ""
    return f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{escape(title)}</title>
        <style>{_base_styles()}</style>
      </head>
      <body>
        <div class="wrap">
          <header>
            <h1>Ploudeville Family Task &amp; Reward System</h1>
            <div class="nav"><a href="/">Home</a><a href="/parent">Parent Panel</a></div>
          </header>
          {msg_html}
          {body}
        </div>
      </body>
    </html>
    """


def _get_form_data(environ: Environ) -> dict[str, str]:
    raw_length = environ.get("CONTENT_LENGTH") or "0"
    try:
        length = int(raw_length)
    except ValueError:
        length = 0
    payload = environ["wsgi.input"].read(length).decode("utf-8") if length > 0 else ""
    parsed = parse_qs(payload, keep_blank_values=True)
    return {k: v[0] for k, v in parsed.items()}


def _query_value(environ: Environ, key: str, default: str = "") -> str:
    query = str(environ.get("QUERY_STRING") or "")
    parsed = parse_qs(query)
    return parsed.get(key, [default])[0]


def _balances_for_child(child_id: int) -> dict[str, float]:
    balances = {"allowance": 0.0, "screen_time": 0.0, "points": 0.0}
    for row in list_balances(child_id=child_id):
        balances[str(row["asset_type"])] = float(row["balance"])
    return balances


def _home_page(msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    children = list_children()
    rules_rows = list_child_house_rules(active_only=True)
    rules_by_child = {int(r["child_id"]): r for r in rules_rows}
    reminders = get_after_school_reminders()
    motd = get_message_of_the_day()
    fun_fact = get_fun_fact_of_the_day()
    total_adoptions = count_pet_adoptions()
    screen_allot_today = get_screen_time_allotment_minutes()
    daily_interest_rate = get_wallet_daily_interest_rate_percent()
    family_interest_total = total_interest_earned()
    birthday_names = [str(c["name"]) for c in list_today_birthdays()]
    cards = []
    schedule_cards = []
    for child in children:
        activities_today = get_child_activities_for_date(int(child["id"]))
        balances = _balances_for_child(int(child["id"]))
        donated_total = total_completed_donations(int(child["id"]))
        interest_total = total_interest_earned(int(child["id"]))
        service_hours = total_completed_service_hours(int(child["id"]))
        child_rules = rules_by_child.get(int(child["id"])) or {}
        pin_state = "PIN enabled" if child.get("has_pin") else "PIN not set"
        adopted_count = count_pet_adoptions(int(child["id"]))
        birthday_note = "<p class='muted'>Birthday today! Special treatment active.</p>" if str(child["name"]) in birthday_names else ""
        cards.append(
            f"""
            <article class="card">
              <h3>{escape(str(child['name']))} (Age {child['age']})</h3>
              <p class="muted">{pin_state}</p>
              {birthday_note}
              <p class="muted">Pets adopted: {adopted_count}</p>
              <p class="muted">Wallet ${balances['allowance']:.2f} | Screen {balances['screen_time']:.0f} min</p>
              <p class="muted">Interest earned: ${interest_total:.2f}</p>
              <p class="muted">Screen allotment today: {screen_allot_today} min</p>
              <p class="muted">Donated to charities: ${donated_total:.2f}</p>
              <p class="muted">Lifetime service hours: {service_hours:.2f}</p>
              <a href="/child?child_id={child['id']}">Open Child Portal</a>
            </article>
            """
        )
        schedule_cards.append(
            f"""
            <article class="card">
              <h3>{escape(str(child['name']))}</h3>
              <p><strong>Today's Schedule</strong></p>
              <p class="muted">Sun-Thu: screen off {escape(str(child_rules.get('weekday_screen_off') or 'not set'))}, bedtime {escape(str(child_rules.get('weekday_bedtime') or 'not set'))}</p>
              <p class="muted">Fri-Sat: screen off {escape(str(child_rules.get('weekend_screen_off') or 'not set'))}, bedtime {escape(str(child_rules.get('weekend_bedtime') or 'not set'))}</p>
              <p class="muted">{' | '.join(escape(str((a['start_time'] + ' ') if a.get('start_time') else '') + str(a['activity_name'])) for a in activities_today) if activities_today else 'No activities scheduled.'}</p>
            </article>
            """
        )
    birthday_banner = (
        f"<p class='msg'>Birthday spotlight: {', '.join(escape(n) for n in birthday_names)}</p>"
        if birthday_names
        else ""
    )
    body = (
        f"<section>"
        f"{birthday_banner}"
        f"<div class='motd-row'>"
        f"<div class='card'><h3>Message of the Day</h3><p class='muted truncate' title=\"{escape(str(motd['message_text'])) if motd else 'No message set for today.'}\">{escape(str(motd['message_text'])) if motd else 'No message set for today.'}</p></div>"
        f"<div class='card'><h3>Fun Fact of the Day</h3><p class='muted truncate' title=\"{escape(str(fun_fact['fact_text'])) if fun_fact else 'No fun fact set for today.'}\">{escape(str(fun_fact['fact_text'])) if fun_fact else 'No fun fact set for today.'}</p></div>"
        f"</div>"
        f"<div class='card'><h3>After School Reminders</h3><p class='muted'>{', '.join(escape(r) for r in reminders) if reminders else 'No reminders set.'}</p></div>"
        f"<h2>Choose a Child Portal</h2>"
        f"<p class='muted'>Daily wallet interest rate: {daily_interest_rate:.4f}% | Total interest earned: ${family_interest_total:.2f}</p>"
        f"<p class='muted'>Total pets adopted in system: {total_adoptions}</p>"
        f"<div class='grid'>{''.join(cards) if cards else '<p>No children configured.</p>'}</div></section>"
        f"<section><h2>Today's Schedule</h2><div class='grid'>{''.join(schedule_cards) if schedule_cards else '<p>No schedules configured.</p>'}</div></section>"
    )
    return _html_response(_layout("Home", body, msg))


def _child_login_page(child_id: int, msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    child = get_child(child_id)
    if not child:
        return _html_response(_layout("Not Found", "<h2>Child not found</h2>"), status="404 Not Found")
    if not child.get("pin_hash"):
        body = f"""
        <section class="card">
          <h2>{escape(str(child['name']))}'s Portal</h2>
          <p>PIN has not been set yet. Ask a parent to set it in Parent Panel.</p>
        </section>
        """
        return _html_response(_layout("PIN Required", body, msg))

    body = f"""
    <section class="card">
      <h2>{escape(str(child['name']))}'s PIN Login</h2>
      <form method="post" action="/child-login">
        <input type="hidden" name="child_id" value="{child_id}" />
        <input type="password" name="pin" inputmode="numeric" placeholder="Enter PIN" required />
        <button type="submit">Unlock</button>
      </form>
    </section>
    """
    return _html_response(_layout("Child Login", body, msg))


def _parent_login_page(msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    reset_emails = get_parent_reset_emails()
    reset_texts = get_parent_reset_text_numbers()
    body = f"""
    <section class="card">
      <h2>Parent Panel Sign In</h2>
      <form method="post" action="/parent-login">
        <input type="password" name="password" placeholder="Parent password" required />
        <button type="submit">Sign In</button>
      </form>
    </section>
    <section class="card">
      <h3>Reset Password</h3>
      <p class="muted">Reset emails are sent to {escape(', '.join(reset_emails))}</p>
      <form method="post" action="/parent-request-reset">
        <button type="submit">Email Reset Link</button>
      </form>
      <p class="muted">Reset texts are sent to {escape(', '.join(reset_texts) if reset_texts else 'No parent text numbers set')}</p>
      <form method="post" action="/parent-request-reset-text">
        <button type="submit">Text Reset Link</button>
      </form>
    </section>
    """
    return _html_response(_layout("Parent Login", body, msg))


def _parent_reset_page(token: str, msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    body = f"""
    <section class="card">
      <h2>Reset Parent Password</h2>
      <form method="post" action="/parent-reset">
        <input type="hidden" name="token" value="{escape(token)}" />
        <input type="password" name="new_password" placeholder="New password" required />
        <button type="submit">Set New Password</button>
      </form>
      <p class="muted"><a href="/parent-login">Back to parent login</a></p>
    </section>
    """
    return _html_response(_layout("Parent Reset", body, msg))


def _child_pin_reset_page(token: str, msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    body = f"""
    <section class="card">
      <h2>Reset Child PIN</h2>
      <form method="post" action="/child-pin-reset">
        <input type="hidden" name="token" value="{escape(token)}" />
        <input type="password" name="new_pin" inputmode="numeric" placeholder="New PIN (4-8 digits)" required />
        <button type="submit">Set New PIN</button>
      </form>
      <p class="muted"><a href="/parent">Back to parent panel</a></p>
    </section>
    """
    return _html_response(_layout("Child PIN Reset", body, msg))


def _child_page(child_id: int, msg: str = "") -> tuple[str, list[tuple[str, str]], bytes]:
    child = get_child(child_id)
    if not child:
        return _html_response(_layout("Not Found", "<h2>Child not found.</h2>"), status="404 Not Found")

    generate_pet_help_messages(child_id)
    pet_info = get_pet_weekly_dashboard(child_id)
    pet_species = list_pet_species()
    adoptions = list_pet_adoptions(child_id=child_id, limit=20)
    badges = list_pet_badges(child_id=child_id, limit=20)
    balances = _balances_for_child(child_id)
    screen_allot_today = get_screen_time_allotment_minutes()
    due_items = list_due_task_instances(child_id=child_id)
    rewards = list_rewards()
    charities = list_charities(limit=200)
    donation_pledges = list_donation_pledges(child_id=child_id, limit=50)
    wallet_payouts = list_wallet_payouts(child_id=child_id, limit=50)
    service_orgs = list_service_organizations(active_only=True, limit=200)
    service_entries = list_service_entries(child_id=child_id, limit=50)
    service_hours_total = total_completed_service_hours(child_id)
    today_activities = get_child_activities_for_date(child_id)
    app_limits = list_child_app_limits(child_id=child_id, active_only=True)
    concert_goals = list_child_concert_goal_progress(child_id=child_id)
    adventure_goals = list_child_adventure_goal_progress(child_id=child_id)
    reading_logs = list_reading_logs(child_id=child_id, limit=30)
    available_allowance = get_available_allowance_for_payout(child_id)
    pending = list_task_completions(status="pending", child_id=child_id)
    messages = list_messages(child_id=child_id, limit=50)

    due_rows = []
    for item in due_items:
        action = ""
        if item["status"] in ("open", "rejected"):
            action = (
                f"<form method='post' action='/submit-instance'>"
                f"<input type='hidden' name='child_id' value='{child_id}' />"
                f"<input type='hidden' name='instance_id' value='{item['id']}' />"
                f"<input type='text' name='note' placeholder='Optional note' />"
                f"<button type='submit'>Submit</button></form>"
            )
        due_rows.append(
            f"<tr><td>{escape(str(item['task_name']))}</td><td>{escape(str(item['due_date']))}</td><td>{escape(str(item['status']))}</td><td>{action or 'Waiting for review'}</td></tr>"
        )

    reward_rows = "".join(
        f"<tr><td>{escape(str(r['name']))}</td><td>{escape(str(r['reward_type']))}</td><td>{float(r['cost']):.2f}</td></tr>"
        for r in rewards
    )
    pending_rows = "".join(
        f"<tr><td>{p['id']}</td><td>{escape(str(p['task_name']))}</td><td>{escape(str(p.get('due_date') or ''))}</td><td>{escape(str(p['status']))}</td></tr>"
        for p in pending
    )
    message_rows = "".join(
        f"<tr><td>{escape(str(m['created_at']))}</td><td>{escape(str(m['sender_name']))}</td><td>{escape(str(m['message_text']))}</td></tr>"
        for m in messages
    )
    charity_rows = "".join(
        (
            f"<tr><td>{escape(str(ch['name']))}</td>"
            f"<td><a href='{escape(str(ch['website']))}' target='_blank' rel='noopener'>site</a></td>"
            f"<td>{'yes' if int(ch['website_live']) else 'check needed'}</td>"
            f"<td>{'verified' if int(ch['tax_exempt_verified']) else 'pending parent review'}</td>"
            f"<td>{escape(str(ch.get('created_by_child_name') or 'System'))}</td></tr>"
        )
        for ch in charities
    )
    donation_rows = "".join(
        (
            f"<tr><td>{escape(str(d['created_at']))}</td>"
            f"<td>{escape(str(d['charity_name']))}</td>"
            f"<td>${float(d['amount']):.2f}</td>"
            f"<td>{escape(str(d['status']))}</td>"
            f"<td>{escape(str(d.get('completed_by') or '-'))}</td></tr>"
        )
        for d in donation_pledges
    )
    wallet_rows = "".join(
        (
            f"<tr><td>{escape(str(w['created_at']))}</td>"
            f"<td>${float(w['amount']):.2f}</td>"
            f"<td>{escape(str(w['status']))}</td>"
            f"<td>{escape(str(w.get('sent_by') or '-'))}</td>"
            f"<td>{escape(str(w.get('transfer_reference') or '-'))}</td></tr>"
        )
        for w in wallet_payouts
    )
    service_rows = "".join(
        (
            f"<tr><td>{escape(str(s['service_date']))}</td>"
            f"<td>{escape(str(s['organization_name']))}</td>"
            f"<td>{float(s['hours']):.2f}</td>"
            f"<td>{escape(str(s['status']))}</td>"
            f"<td>{escape(str(s.get('reviewed_by') or '-'))}</td></tr>"
        )
        for s in service_entries
    )
    activity_rows = "".join(
        (
            f"<tr><td>{escape(str(a.get('start_time') or '-'))}</td>"
            f"<td>{escape(str(a['activity_name']))}</td>"
            f"<td>{escape(str(a.get('end_time') or '-'))}</td>"
            f"<td>{escape(str(a.get('category') or '-'))}</td></tr>"
        )
        for a in today_activities
    )
    app_limit_rows = "".join(
        f"<tr><td>{escape(str(a['app_name']))}</td><td>{int(a['minutes_per_day'])} min/day</td></tr>"
        for a in app_limits
    )
    concert_rows = "".join(
        (
            f"<tr><td>{escape(str(cg['artist_name']))}</td>"
            f"<td>{escape(str(cg['event_name']))}</td>"
            f"<td>{escape(str(cg['event_date']))}</td>"
            f"<td>${float(cg['low_price']):.2f}-${float(cg['high_price']):.2f}</td>"
            f"<td>${float(cg['kid_target_amount']):.2f}</td>"
            f"<td>{float(cg['progress_percent']):.0f}%</td>"
            f"<td>{'Yes' if bool(cg['eligible']) else 'Not yet'}</td></tr>"
        )
        for cg in concert_goals
    )
    adventure_rows = "".join(
        (
            f"<tr><td>{escape(str(ag['park_name']))}</td>"
            f"<td>{escape(str(ag['ticket_name']))}</td>"
            f"<td>{escape(str(ag['target_date']))}</td>"
            f"<td>{escape(str(ag.get('region') or '-'))}</td>"
            f"<td>${float(ag['low_price']):.2f}-${float(ag['high_price']):.2f}</td>"
            f"<td>${float(ag['kid_target_amount']):.2f}</td>"
            f"<td>{float(ag['progress_percent']):.0f}%</td>"
            f"<td>{'Yes' if bool(ag['eligible']) else 'Not yet'}</td></tr>"
        )
        for ag in adventure_goals
    )
    reading_rows = "".join(
        (
            f"<tr><td>{escape(str(r['read_date']))}</td>"
            f"<td>{escape(str(r['book_title']))}</td>"
            f"<td>{escape(str(r['chapters']))}</td>"
            f"<td>{escape(str(r['start_time']))}-{escape(str(r['end_time']))}</td>"
            f"<td>{escape(str(r['status']))}</td>"
            f"<td>{'yes' if int(r.get('passed') or 0) else 'no'}</td></tr>"
        )
        for r in reading_logs
    )
    pending_quiz = next((r for r in reading_logs if str(r.get("status")) in ("awaiting_answers", "failed")), None)
    reading_rows_html = reading_rows if reading_rows else '<tr><td colspan="6">No reading logs yet.</td></tr>'
    pending_quiz_html = (
        (
            "<form method='post' action='/answer-reading-quiz'>"
            f"<input type='hidden' name='child_id' value='{child_id}' />"
            f"<input type='hidden' name='log_id' value='{pending_quiz['id']}' />"
            f"<p><strong>Q1:</strong> {escape(str(pending_quiz['question_1']))}</p>"
            "<input name='answer_1' placeholder='Answer question 1' required style='min-width:420px;' />"
            f"<p><strong>Q2:</strong> {escape(str(pending_quiz['question_2']))}</p>"
            "<input name='answer_2' placeholder='Answer question 2' required style='min-width:420px;' />"
            "<button type='submit'>Submit Quiz Answers</button>"
            "</form>"
        )
        if pending_quiz
        else "<p class='muted'>No pending reading quiz right now.</p>"
    )
    care = pet_info["care"]
    pet_name = pet_info["pet"]["pet_name"] if pet_info["pet"] else "No pet yet"
    pet_species_name = pet_info["pet"]["pet_species_name"] if pet_info["pet"] else "-"
    birthday_note = (
        "<p class='msg'>Happy Birthday! Your special day treatment has been applied.</p>"
        if any(int(c["id"]) == child_id for c in list_today_birthdays())
        else ""
    )
    adoption_rows = "".join(
        f"<tr><td>{escape(str(a['week_key']))}</td><td>{escape(str(a['pet_name']))}</td><td>{escape(str(a['pet_species_name']))}</td></tr>"
        for a in adoptions
    )
    badge_rows = "".join(
        f"<tr><td>{escape(str(b['week_key']))}</td><td>{escape(str(b['badge_name']))}</td><td>{escape(str(b['description']))}</td></tr>"
        for b in badges
    )

    body = f"""
    <section class="two-col">
      <div>
        <h2>{escape(str(child['name']))}'s Portal</h2>
        {birthday_note}
        <p class="muted">Allowance ${balances['allowance']:.2f} | Screen {balances['screen_time']:.0f} min | Points {balances['points']:.0f}</p>
        <p class="muted">Screen allotment today: {screen_allot_today} min</p>

        <div class="card">
          <h3>Pet Center ({pet_info['week_key']})</h3>
          <p class="muted">Current: {escape(str(pet_name))} the {escape(str(pet_species_name))} ({escape(str(pet_info['health']))})</p>
          <p class="muted">Required tasks this week: {pet_info['required_completed']} / {pet_info['required_minimum']}</p>
          <p class="muted">Care status: feed={care['feed']}, water={care['water']}, nurture={care['nurture']}</p>
          <p class="muted">Care streak: {pet_info['care_streak_weeks']} week(s)</p>
          <form method="post" action="/adopt-pet">
            <input type="hidden" name="child_id" value="{child_id}" />
            <select name="pet_species_id" required>
              <option value="">Choose pet species</option>
              {''.join(f'<option value="{p["id"]}">{escape(str(p["name"]))} ({escape(str(p["rarity"]))})</option>' for p in pet_species)}
            </select>
            <input name="pet_name" placeholder="Name your pet" required />
            <button type="submit">Choose This Week's Pet</button>
          </form>
          <form method="post" action="/set-default-pet">
            <input type="hidden" name="child_id" value="{child_id}" />
            <select name="pet_species_id" required>
              <option value="">Set default species</option>
              {''.join(f'<option value="{p["id"]}">{escape(str(p["name"]))}</option>' for p in pet_species)}
            </select>
            <input name="pet_name" placeholder="Default pet name" required />
            <button type="submit">Set Default Pet</button>
          </form>
          <form method="post" action="/pet-care">
            <input type="hidden" name="child_id" value="{child_id}" />
            <button type="submit" name="care_type" value="feed">Feed</button>
            <button type="submit" name="care_type" value="water">Water</button>
            <button type="submit" name="care_type" value="nurture">Nurture</button>
          </form>
          <form method="post" action="/create-pet-species">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input type="hidden" name="sender_scope" value="kid" />
            <input name="pet_name" placeholder="Create new pet species" required />
            <select name="rarity">
              <option value="common">common</option>
              <option value="uncommon">uncommon</option>
              <option value="rare">rare</option>
              <option value="mythic">mythic</option>
            </select>
            <button type="submit">Add Species</button>
          </form>
          <h4>Available Pets</h4>
          <table>
            <thead><tr><th>Species</th><th>Rarity</th><th>Creator</th></tr></thead>
            <tbody>{''.join(f'<tr><td>{escape(str(p["name"]))}</td><td>{escape(str(p["rarity"]))}</td><td>{escape(str(p.get("created_by_child_name") or "System"))}</td></tr>' for p in pet_species)}</tbody>
          </table>
          <h4>My Adopted Pets</h4>
          <table>
            <thead><tr><th>Week</th><th>Name</th><th>Species</th></tr></thead>
            <tbody>{adoption_rows if adoption_rows else '<tr><td colspan="3">No adoptions yet.</td></tr>'}</tbody>
          </table>
          <h4>My Badges</h4>
          <table>
            <thead><tr><th>Week</th><th>Badge</th><th>Description</th></tr></thead>
            <tbody>{badge_rows if badge_rows else '<tr><td colspan="3">No badges yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Due Tasks</h3>
          <table>
            <thead><tr><th>Task</th><th>Due Date</th><th>Status</th><th>Action</th></tr></thead>
            <tbody>{''.join(due_rows) if due_rows else '<tr><td colspan="4">No scheduled tasks due.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Pending Reviews</h3>
          <table>
            <thead><tr><th>ID</th><th>Task</th><th>Due Date</th><th>Status</th></tr></thead>
            <tbody>{pending_rows if pending_rows else '<tr><td colspan="4">No pending submissions.</td></tr>'}</tbody>
          </table>
        </div>
      </div>

      <div>
        <div class="card">
          <h3>Redeem Reward</h3>
          <form method="post" action="/redeem-reward">
            <input type="hidden" name="child_id" value="{child_id}" />
            <select name="reward_id" required>
              <option value="">Select reward</option>
              {''.join(f'<option value="{r["id"]}">{escape(str(r["name"]))} ({escape(str(r["reward_type"]))} {float(r["cost"]):.2f})</option>' for r in rewards)}
            </select>
            <input type="text" name="note" placeholder="Optional note" />
            <button type="submit">Redeem</button>
          </form>
        </div>

        <div class="card">
          <h3>Wallet</h3>
          <p class="muted">Available allowance for payout: ${available_allowance:.2f}</p>
          <p class="muted">Create a payout request and a parent can mark it sent to Apple Cash.</p>
          <form method="post" action="/request-wallet-payout">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input type="number" name="amount" min="0.01" step="0.01" placeholder="Amount" required />
            <input name="note" placeholder="Optional note" />
            <button type="submit">Request Payout</button>
          </form>
          <table>
            <thead><tr><th>Requested</th><th>Amount</th><th>Status</th><th>Sent By</th><th>Reference</th></tr></thead>
            <tbody>{wallet_rows if wallet_rows else '<tr><td colspan="5">No payout requests yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Service Hours</h3>
          <p class="muted">Lifetime completed service hours: {service_hours_total:.2f}</p>
          <form method="post" action="/submit-service-hours">
            <input type="hidden" name="child_id" value="{child_id}" />
            <select name="organization_id" required>
              <option value="">Choose organization</option>
              {''.join(f'<option value="{o["id"]}">{escape(str(o["name"]))}</option>' for o in service_orgs)}
            </select>
            <input type="number" name="hours" min="0.25" step="0.25" placeholder="Hours" required />
            <input type="date" name="service_date" />
            <input name="note" placeholder="What did you do?" />
            <button type="submit">Submit Service</button>
          </form>
          <form method="post" action="/add-service-organization">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input type="hidden" name="sender_scope" value="kid" />
            <input name="name" placeholder="Add volunteer organization" required />
            <input name="website" placeholder="https://org.org" />
            <button type="submit">Add Organization</button>
          </form>
          <h4>My Service Entries</h4>
          <table>
            <thead><tr><th>Date</th><th>Organization</th><th>Hours</th><th>Status</th><th>Reviewed By</th></tr></thead>
            <tbody>{service_rows if service_rows else '<tr><td colspan="5">No service entries yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Rewards Catalog</h3>
          <table>
            <thead><tr><th>Reward</th><th>Type</th><th>Cost</th></tr></thead>
            <tbody>{reward_rows if reward_rows else '<tr><td colspan="3">No rewards configured.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>App Time Limits</h3>
          <table>
            <thead><tr><th>App</th><th>Limit</th></tr></thead>
            <tbody>{app_limit_rows if app_limit_rows else '<tr><td colspan="2">No app limits configured.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Concert Rewards (80/20)</h3>
          <p class="muted">If you save 80% of the low ticket price, parents cover 20%.</p>
          <table>
            <thead><tr><th>Artist</th><th>Event</th><th>Date</th><th>Price Range</th><th>Your Goal</th><th>Progress</th><th>Eligible</th></tr></thead>
            <tbody>{concert_rows if concert_rows else '<tr><td colspan="7">No concert goals added yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Adventure Parks (80/20)</h3>
          <p class="muted">Save 80% of the low ticket price and parents cover 20%.</p>
          <table>
            <thead><tr><th>Park</th><th>Ticket</th><th>Target Date</th><th>Region</th><th>Price Range</th><th>Your Goal</th><th>Progress</th><th>Eligible</th></tr></thead>
            <tbody>{adventure_rows if adventure_rows else '<tr><td colspan="8">No adventure park goals added yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Reading Log + Quiz</h3>
          <p class="muted">Log your reading, answer 2 questions, and pass to earn reading task credit.</p>
          <form method="post" action="/submit-reading-log">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input type="date" name="read_date" value="{date.today().isoformat()}" required />
            <input type="time" name="start_time" required />
            <input type="time" name="end_time" required />
            <input name="book_title" placeholder="Book title" required />
            <input name="chapters" placeholder="Chapters/pages (ex: 4-6)" required />
            <button type="submit">Save Reading + Get Questions</button>
          </form>
          {pending_quiz_html}
          <h4>Recent Reading Logs</h4>
          <table>
            <thead><tr><th>Date</th><th>Book</th><th>Chapters</th><th>Time</th><th>Status</th><th>Passed</th></tr></thead>
            <tbody>{reading_rows_html}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Charity &amp; Donations</h3>
          <p class="muted">Donations are completed by parents and deducted from your allowance balance when marked complete.</p>
          <h4>Charity List</h4>
          <table>
            <thead><tr><th>Name</th><th>Website</th><th>Site Live</th><th>Tax Exempt</th><th>Added By</th></tr></thead>
            <tbody>{charity_rows if charity_rows else '<tr><td colspan="5">No charities yet.</td></tr>'}</tbody>
          </table>
          <form method="post" action="/submit-charity">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input name="charity_name" placeholder="Charity name" required />
            <input name="charity_website" placeholder="https://example.org" required />
            <button type="submit">Suggest Charity</button>
          </form>
          <form method="post" action="/create-donation-pledge">
            <input type="hidden" name="child_id" value="{child_id}" />
            <select name="charity_id" required>
              <option value="">Choose charity</option>
              {''.join(f'<option value="{ch["id"]}">{escape(str(ch["name"]))}</option>' for ch in charities)}
            </select>
            <input type="number" name="amount" min="0.01" step="0.01" placeholder="Amount" required />
            <input name="note" placeholder="Optional note (ex: Save the whales)" />
            <button type="submit">Create Donation Request</button>
          </form>
          <h4>My Donation Requests</h4>
          <table>
            <thead><tr><th>When</th><th>Charity</th><th>Amount</th><th>Status</th><th>Completed By</th></tr></thead>
            <tbody>{donation_rows if donation_rows else '<tr><td colspan="5">No donation requests yet.</td></tr>'}</tbody>
          </table>
        </div>

        <div class="card">
          <h3>Messages</h3>
          <table>
            <thead><tr><th>When</th><th>From</th><th>Message</th></tr></thead>
            <tbody>{message_rows if message_rows else '<tr><td colspan="3">No messages yet.</td></tr>'}</tbody>
          </table>
          <form method="post" action="/post-message">
            <input type="hidden" name="child_id" value="{child_id}" />
            <input type="hidden" name="sender_type" value="kid" />
            <input type="hidden" name="sender_name" value="{escape(str(child['name']))}" />
            <input name="message_text" placeholder="Send message to parents/pet..." required />
            <button type="submit">Send</button>
          </form>
        </div>

        <form method="post" action="/child-logout"><input type="hidden" name="child_id" value="{child_id}" /><button type="submit">Lock Portal</button></form>
      </div>
    </section>
    <section class="card">
      <h2><strong>Today's Schedule</strong></h2>
      <table>
        <thead><tr><th>Start</th><th>Activity</th><th>End</th><th>Type</th></tr></thead>
        <tbody>{activity_rows if activity_rows else '<tr><td colspan="4">No activities scheduled for today.</td></tr>'}</tbody>
      </table>
    </section>
    """
    return _html_response(_layout(f"{child['name']} Portal", body, msg))


def _parent_page(
    msg: str = "",
    include_inactive: bool = True,
    concert_artist: str = "",
    concert_city: str = "",
    concert_state: str = "",
    interest_child_id: str = "",
    interest_date_from: str = "",
    interest_date_to: str = "",
    interest_preset: str = "",
    reading_child_id: str = "",
    reading_status: str = "",
    reading_date_from: str = "",
    reading_date_to: str = "",
) -> tuple[str, list[tuple[str, str]], bytes]:
    children = list_children()
    tasks = list_tasks()
    rewards = list_rewards()
    pet_species = list_pet_species()
    pet_adoptions = list_pet_adoptions(limit=100)
    pet_badges = list_pet_badges(limit=100)
    parents = list_parents(active_only=True)
    parent_birthdays = list_today_parent_birthdays()
    schedules = list_task_schedules(active_only=not include_inactive)
    balances = list_balances()
    preset = interest_preset.strip().lower()
    interest_from_value = interest_date_from.strip()
    interest_to_value = interest_date_to.strip()
    if preset in {"last7", "this_month", "ytd"}:
        today = date.today()
        if preset == "last7":
            interest_from_value = (today - timedelta(days=6)).isoformat()
            interest_to_value = today.isoformat()
        elif preset == "this_month":
            interest_from_value = date(today.year, today.month, 1).isoformat()
            interest_to_value = today.isoformat()
        elif preset == "ytd":
            interest_from_value = date(today.year, 1, 1).isoformat()
            interest_to_value = today.isoformat()
    filter_child_id = int(interest_child_id) if interest_child_id.strip().isdigit() else None
    interest_accruals = list_interest_accruals(
        child_id=filter_child_id,
        date_from=interest_from_value or None,
        date_to=interest_to_value or None,
        limit=500,
    )
    wallet_interest_rate = get_wallet_daily_interest_rate_percent()
    family_interest_total = total_interest_earned()
    charities = list_charities(limit=300)
    donation_pledges = list_donation_pledges(limit=300)
    wallet_payouts = list_wallet_payouts(limit=300)
    service_orgs = list_service_organizations(active_only=True, limit=300)
    service_entries = list_service_entries(limit=300)
    service_rates = get_service_credit_rates()
    house_rules = list_child_house_rules(active_only=True)
    reminders = get_after_school_reminders()
    motd_today = get_message_of_the_day()
    motd_library = list_motd_library(active_only=True, limit=250)
    fun_fact_today = get_fun_fact_of_the_day()
    fun_fact_library = list_fun_fact_library(active_only=True, limit=500)
    holidays = list_holidays(limit=120)
    all_activities = list_child_activities(active_only=True, limit=500)
    all_app_limits = list_child_app_limits(child_id=None, active_only=False)
    concert_goals_all = list_concert_goals(child_id=None, active_only=False)
    adventure_catalog = list_adventure_park_catalog(active_only=True)
    adventure_goals_all = list_adventure_goals(child_id=None, active_only=False)
    reading_child_filter_id = int(reading_child_id) if reading_child_id.strip().isdigit() else None
    reading_status_value = reading_status.strip()
    reading_date_from_value = reading_date_from.strip()
    reading_date_to_value = reading_date_to.strip()
    reading_logs_all = list_reading_logs(
        child_id=reading_child_filter_id,
        status=reading_status_value or None,
        date_from=reading_date_from_value or None,
        date_to=reading_date_to_value or None,
        limit=400,
    )
    ios_reports = list_ios_usage_reports(limit=200)
    notification_attempts = list_activity_notification_attempts(limit=150)
    concert_search_results: list[dict] = []
    concert_search_error = ""
    if concert_artist.strip():
        try:
            concert_search_results = search_ticketmaster_events(
                artist_keyword=concert_artist,
                city=concert_city,
                state_code=concert_state,
                size=20,
            )
        except Exception as err:  # noqa: BLE001
            concert_search_error = str(err)
    pending = list_task_completions(status="pending")
    messages = list_messages(limit=100)

    pending_rows = "".join(
        f"""
        <tr>
          <td>{p['id']}</td><td>{escape(str(p['child_name']))}</td><td>{escape(str(p['task_name']))}</td><td>{escape(str(p.get('due_date') or ''))}</td>
          <td>
            <form method="post" action="/review-completion">
              <input type="hidden" name="id" value="{p['id']}" />
              <input type="text" name="by" placeholder="Parent" required />
              <button type="submit" name="decision" value="approved">Approve</button>
              <button type="submit" name="decision" value="rejected">Reject</button>
            </form>
          </td>
        </tr>
        """
        for p in pending
    )

    schedule_rows = "".join(
        f"""
        <tr>
          <td>{s['id']}</td>
          <td>{escape(str(s['task_name']))}</td>
          <td>{escape(str(s['child_name'] or 'All'))}</td>
          <td>{escape(str(s['cadence']))}</td>
          <td>{'' if s['day_of_week'] is None else s['day_of_week']}</td>
          <td>{escape(str(s['due_time'] or ''))}</td>
          <td>{'active' if s['active'] else 'inactive'}</td>
          <td>
            <form method="post" action="/toggle-schedule-active">
              <input type="hidden" name="schedule_id" value="{s['id']}" />
              <input type="hidden" name="show_inactive" value="1" />
              <input type="hidden" name="active" value="{0 if s['active'] else 1}" />
              <button type="submit">{'Disable' if s['active'] else 'Enable'}</button>
            </form>
            <form method="post" action="/update-schedule">
              <input type="hidden" name="schedule_id" value="{s['id']}" />
              <input type="hidden" name="show_inactive" value="1" />
              <select name="task_id" required>
                {''.join(f'<option value="{t["id"]}" {"selected" if t["id"] == s["task_id"] else ""}>{escape(str(t["name"]))}</option>' for t in tasks)}
              </select>
              <select name="child_id">
                <option value="" {"selected" if s["child_id"] is None else ""}>All</option>
                {''.join(f'<option value="{c["id"]}" {"selected" if c["id"] == s["child_id"] else ""}>{escape(str(c["name"]))}</option>' for c in children)}
              </select>
              <select name="cadence">
                <option value="daily" {"selected" if s["cadence"] == "daily" else ""}>daily</option>
                <option value="weekly" {"selected" if s["cadence"] == "weekly" else ""}>weekly</option>
              </select>
              <input type="number" name="day_of_week" min="0" max="6" value="{'' if s['day_of_week'] is None else s['day_of_week']}" />
              <input name="due_time" value="{escape(str(s['due_time'] or ''))}" placeholder="HH:MM" />
              <button type="submit">Save Edit</button>
            </form>
            <form method="post" action="/delete-schedule">
              <input type="hidden" name="schedule_id" value="{s['id']}" />
              <input type="hidden" name="show_inactive" value="1" />
              <button type="submit">Delete</button>
            </form>
          </td>
        </tr>
        """
        for s in schedules
    )
    balance_rows = "".join(
        f"<tr><td>{escape(str(b['child_name']))}</td><td>{escape(str(b['asset_type']))}</td><td>{float(b['balance']):.2f}</td></tr>"
        for b in balances
    )
    message_rows = "".join(
        f"<tr><td>{escape(str(m['created_at']))}</td><td>{escape(str(m['sender_name']))}</td><td>{escape(str(m.get('child_name') or 'All'))}</td><td>{escape(str(m['message_text']))}</td></tr>"
        for m in messages
    )
    pet_species_rows = "".join(
        f"<tr><td>{escape(str(p['name']))}</td><td>{escape(str(p['rarity']))}</td><td>{escape(str(p.get('created_by_child_name') or 'System'))}</td></tr>"
        for p in pet_species
    )
    pet_adoption_rows = "".join(
        f"<tr><td>{escape(str(a['week_key']))}</td><td>{escape(str(a['child_name']))}</td><td>{escape(str(a['pet_name']))}</td><td>{escape(str(a['pet_species_name']))}</td></tr>"
        for a in pet_adoptions
    )
    pet_badge_rows = "".join(
        f"<tr><td>{escape(str(b['week_key']))}</td><td>{escape(str(b['child_name']))}</td><td>{escape(str(b['badge_name']))}</td></tr>"
        for b in pet_badges
    )
    parent_rows = "".join(
        f"<tr><td>{escape(str(p['name']))}</td><td>{escape(str(p['email']))}</td><td>{escape(str(p.get('birthdate') or ''))}</td></tr>"
        for p in parents
    )
    charity_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(ch['name']))}</td>
          <td><a href="{escape(str(ch['website']))}" target="_blank" rel="noopener">visit</a></td>
          <td>{'yes' if int(ch['website_live']) else 'no'}</td>
          <td>{'yes' if int(ch['tax_exempt_verified']) else 'pending'}</td>
          <td>{escape(str(ch.get('verified_by_parent') or '-'))}</td>
          <td>{escape(str(ch.get('created_by_child_name') or 'System'))}</td>
          <td>
            <form method="post" action="/recheck-charity-site">
              <input type="hidden" name="charity_id" value="{ch['id']}" />
              <button type="submit">Recheck Site</button>
            </form>
            {"<form method='post' action='/verify-charity'><input type='hidden' name='charity_id' value='" + str(ch['id']) + "' /><input name='by' placeholder='Parent' required /><button type='submit'>Mark Tax-Exempt Verified</button></form>" if not int(ch['tax_exempt_verified']) else ""}
          </td>
        </tr>
        """
        for ch in charities
    )
    donation_rows = "".join(
        f"""
        <tr>
          <td>{d['id']}</td>
          <td>{escape(str(d['child_name']))}</td>
          <td>{escape(str(d['charity_name']))}</td>
          <td>${float(d['amount']):.2f}</td>
          <td>{escape(str(d['status']))}</td>
          <td>{escape(str(d.get('note') or '-'))}</td>
          <td>
            {"<form method='post' action='/complete-donation'><input type='hidden' name='pledge_id' value='" + str(d['id']) + "' /><input name='by' placeholder='Parent' required /><button type='submit'>Completed</button></form>" if str(d['status']) == "pending_parent" else "Completed by " + escape(str(d.get('completed_by') or '-'))}
          </td>
        </tr>
        """
        for d in donation_pledges
    )
    wallet_rows = "".join(
        f"""
        <tr>
          <td>{w['id']}</td>
          <td>{escape(str(w['child_name']))}</td>
          <td>${float(w['amount']):.2f}</td>
          <td>{escape(str(w['status']))}</td>
          <td>{escape(str(w.get('note') or '-'))}</td>
          <td>{escape(str(w.get('sent_by') or '-'))}</td>
          <td>{escape(str(w.get('transfer_reference') or '-'))}</td>
          <td>
            {"<form method='post' action='/mark-wallet-payout-sent'><input type='hidden' name='payout_id' value='" + str(w['id']) + "' /><input name='by' placeholder='Parent' required /><input name='reference' placeholder='Txn ref/receipt' /><button type='submit'>Mark Sent</button></form>" if str(w['status']) == "pending_parent" else "Sent"}
          </td>
        </tr>
        """
        for w in wallet_payouts
    )
    house_rule_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(r['child_name']))}</td>
          <td>
            <form method="post" action="/update-child-house-rules">
              <input type="hidden" name="child_id" value="{r['child_id']}" />
              <input name="weekday_screen_off" value="{escape(str(r.get('weekday_screen_off') or '20:15'))}" placeholder="HH:MM" required />
              <input name="weekday_bedtime" value="{escape(str(r.get('weekday_bedtime') or '21:00'))}" placeholder="HH:MM" required />
              <input name="weekend_screen_off" value="{escape(str(r.get('weekend_screen_off') or '21:00'))}" placeholder="HH:MM" required />
              <input name="weekend_bedtime" value="{escape(str(r.get('weekend_bedtime') or '21:30'))}" placeholder="HH:MM" required />
              <button type="submit">Save</button>
            </form>
          </td>
        </tr>
        """
        for r in house_rules
    )
    service_org_rows = "".join(
        f"<tr><td>{escape(str(o['name']))}</td><td>{escape(str(o.get('website') or '-'))}</td><td>{escape(str(o.get('created_by_child_name') or 'System'))}</td></tr>"
        for o in service_orgs
    )
    service_entry_rows = "".join(
        f"""
        <tr>
          <td>{s['id']}</td>
          <td>{escape(str(s['child_name']))}</td>
          <td>{escape(str(s['organization_name']))}</td>
          <td>{float(s['hours']):.2f}</td>
          <td>{escape(str(s['service_date']))}</td>
          <td>{escape(str(s['status']))}</td>
          <td>
            {"<form method='post' action='/review-service-hours'><input type='hidden' name='entry_id' value='" + str(s['id']) + "' /><input name='by' placeholder='Parent' required /><button type='submit' name='decision' value='completed'>Complete</button><button type='submit' name='decision' value='rejected'>Reject</button></form>" if str(s['status']) == "pending_parent" else escape(str(s.get('reviewed_by') or '-'))}
          </td>
        </tr>
        """
        for s in service_entries
    )
    motd_library_options = "".join(
        f'<option value="{m["id"]}">{escape(str(m["message_text"]))[:160]}</option>'
        for m in motd_library
    )
    fun_fact_library_options = "".join(
        f'<option value="{f["id"]}">{escape(str(f["fact_text"]))[:160]}</option>'
        for f in fun_fact_library
    )
    holiday_rows = "".join(
        f"<tr><td>{escape(str(h['holiday_date']))}</td><td>{escape(str(h['holiday_name']))}</td><td>{'yes' if int(h['no_school']) else 'no'}</td></tr>"
        for h in holidays
    )
    activity_today_rows = []
    for c in children:
        for a in get_child_activities_for_date(int(c["id"])):
            activity_today_rows.append(
                f"<tr><td>{escape(str(c['name']))}</td><td>{escape(str(a.get('start_time') or '-'))}</td><td>{escape(str(a['activity_name']))}</td><td>{escape(str(a.get('category') or '-'))}</td></tr>"
            )
    activity_config_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(a['child_name']))}</td>
          <td>{escape(str(a['activity_name']))}</td>
          <td>{escape(str(a.get('specific_date') or f"weekday {a.get('day_of_week')}"))}</td>
          <td>{escape(str(a['start_time']))}</td>
          <td>
            <form method="post" action="/update-activity-notify">
              <input type="hidden" name="activity_id" value="{a['id']}" />
              <label><input type="checkbox" name="notify_enabled" value="1" {"checked" if int(a.get('notify_enabled') or 0) else ""} /> enabled</label>
              <input type="number" name="notify_minutes_before" min="0" max="1440" value="{int(a.get('notify_minutes_before') or 30)}" />
              <select name="notify_channels">
                <option value="email" {"selected" if str(a.get('notify_channels')) == 'email' else ""}>email</option>
                <option value="sms" {"selected" if str(a.get('notify_channels')) == 'sms' else ""}>sms</option>
                <option value="both" {"selected" if str(a.get('notify_channels')) == 'both' else ""}>both</option>
              </select>
              <button type="submit">Save</button>
            </form>
          </td>
        </tr>
        """
        for a in all_activities
    )
    app_limit_manage_rows = "".join(
        f"<tr><td>{escape(str(a['child_name']))}</td><td>{escape(str(a['app_name']))}</td><td>{int(a['minutes_per_day'])}</td><td>{'active' if int(a['active']) else 'inactive'}</td></tr>"
        for a in all_app_limits
    )
    concert_goal_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(g['child_name']))}</td>
          <td>{escape(str(g['artist_name']))}</td>
          <td>{escape(str(g['event_name']))}</td>
          <td>{escape(str(g['event_date']))}</td>
          <td>${float(g['low_price']):.2f}-${float(g['high_price']):.2f}</td>
          <td>${float(g['kid_target_amount']):.2f}</td>
          <td>${float(g['parent_share_amount']):.2f}</td>
          <td>{escape(str(g.get('goal_status') or 'active'))}</td>
          <td>
            <form method="post" action="/set-concert-goal-status">
              <input type="hidden" name="goal_id" value="{g['id']}" />
              <select name="goal_status">
                <option value="active" {"selected" if str(g.get("goal_status")) == "active" else ""}>active</option>
                <option value="purchased" {"selected" if str(g.get("goal_status")) == "purchased" else ""}>purchased</option>
                <option value="archived" {"selected" if str(g.get("goal_status")) == "archived" else ""}>archived</option>
              </select>
              <button type="submit">Save</button>
            </form>
          </td>
        </tr>
        """
        for g in concert_goals_all
    )
    adventure_catalog_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(p['name']))}</td>
          <td>{escape(str(p['region']))}</td>
          <td>{escape(str(p['category']))}</td>
          <td>${float(p['low_price']):.2f}-${float(p['high_price']):.2f}</td>
          <td><a href="{escape(str(p.get('website') or '#'))}" target="_blank" rel="noopener">link</a></td>
          <td>
            <form method="post" action="/add-adventure-goal">
              <select name="child_id" required><option value="">Child</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
              <input type="hidden" name="park_name" value="{escape(str(p['name']))}" />
              <input type="hidden" name="ticket_name" value="Day Pass" />
              <input type="hidden" name="region" value="{escape(str(p['region']))}" />
              <input type="hidden" name="category" value="{escape(str(p['category']))}" />
              <input type="hidden" name="low_price" value="{escape(str(p['low_price']))}" />
              <input type="hidden" name="high_price" value="{escape(str(p['high_price']))}" />
              <input type="hidden" name="currency" value="{escape(str(p.get('currency') or 'USD'))}" />
              <input type="hidden" name="ticket_url" value="{escape(str(p.get('website') or ''))}" />
              <input type="date" name="target_date" value="{date.today().isoformat()}" required />
              <button type="submit">Add Goal (80/20)</button>
            </form>
          </td>
        </tr>
        """
        for p in adventure_catalog
    )
    adventure_goal_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(g['child_name']))}</td>
          <td>{escape(str(g['park_name']))}</td>
          <td>{escape(str(g['ticket_name']))}</td>
          <td>{escape(str(g['target_date']))}</td>
          <td>{escape(str(g.get('region') or '-'))}</td>
          <td>${float(g['low_price']):.2f}-${float(g['high_price']):.2f}</td>
          <td>${float(g['kid_target_amount']):.2f}</td>
          <td>${float(g['parent_share_amount']):.2f}</td>
          <td>{escape(str(g.get('goal_status') or 'active'))}</td>
          <td>
            <form method="post" action="/set-adventure-goal-status">
              <input type="hidden" name="goal_id" value="{g['id']}" />
              <select name="goal_status">
                <option value="active" {"selected" if str(g.get("goal_status")) == "active" else ""}>active</option>
                <option value="purchased" {"selected" if str(g.get("goal_status")) == "purchased" else ""}>purchased</option>
                <option value="archived" {"selected" if str(g.get("goal_status")) == "archived" else ""}>archived</option>
              </select>
              <button type="submit">Save</button>
            </form>
          </td>
        </tr>
        """
        for g in adventure_goals_all
    )
    concert_search_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(r.get('artist_name') or concert_artist))}</td>
          <td>{escape(str(r.get('event_name') or ''))}</td>
          <td>{escape(str(r.get('event_date') or ''))}</td>
          <td>{escape(str(r.get('venue_name') or ''))}</td>
          <td>{escape(str(r.get('city') or ''))}</td>
          <td>{escape(str(r.get('state_code') or ''))}</td>
          <td>{'$' + format(float(r['low_price']), '.2f') if r.get('low_price') is not None else '-'}</td>
          <td>{'$' + format(float(r['high_price']), '.2f') if r.get('high_price') is not None else '-'}</td>
          <td>
            <form method="post" action="/add-concert-goal">
              <select name="child_id" required><option value="">Child</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
              <input type="hidden" name="artist_name" value="{escape(str(r.get('artist_name') or concert_artist))}" />
              <input type="hidden" name="event_name" value="{escape(str(r.get('event_name') or ''))}" />
              <input type="hidden" name="event_date" value="{escape(str(r.get('event_date') or date.today().isoformat()))}" />
              <input type="hidden" name="venue_name" value="{escape(str(r.get('venue_name') or ''))}" />
              <input type="hidden" name="city" value="{escape(str(r.get('city') or ''))}" />
              <input type="hidden" name="state_code" value="{escape(str(r.get('state_code') or ''))}" />
              <input type="hidden" name="low_price" value="{escape(str(r.get('low_price') if r.get('low_price') is not None else '0'))}" />
              <input type="hidden" name="high_price" value="{escape(str(r.get('high_price') if r.get('high_price') is not None else '0'))}" />
              <input type="hidden" name="currency" value="{escape(str(r.get('currency') or 'USD'))}" />
              <input type="hidden" name="ticket_url" value="{escape(str(r.get('ticket_url') or ''))}" />
              <button type="submit" {"disabled" if r.get('low_price') is None else ""}>Add Goal (80/20)</button>
            </form>
          </td>
        </tr>
        """
        for r in concert_search_results
    )
    ios_report_rows = "".join(
        f"<tr><td>{escape(str(r['usage_date']))}</td><td>{escape(str(r['child_name']))}</td><td>{int(r['total_minutes'])}</td><td><span class='truncate' title=\"{escape(str(r['per_app_json']))}\">{escape(str(r['per_app_json']))}</span></td><td>{escape(str(r['created_at']))}</td></tr>"
        for r in ios_reports
    )
    notification_attempt_rows = "".join(
        f"<tr><td>{escape(str(a['created_at']))}</td><td>{escape(str(a['child_name']))}</td><td>{escape(str(a['activity_name']))}</td><td>{escape(str(a['channel']))}</td><td>{escape(str(a['target']))}</td><td>{'ok' if int(a['success']) else 'failed'}</td><td>{escape(str(a['error_text'] or '-'))}</td></tr>"
        for a in notification_attempts
    )
    def _reading_action_cell(row: dict) -> str:
        if row.get("credit_completion_id"):
            return "Credited"
        return (
            "<form method='post' action='/override-reading-credit'>"
            f"<input type='hidden' name='log_id' value='{int(row['id'])}' />"
            "<input name='by' placeholder='Parent' required />"
            "<button type='submit'>Override + Award</button>"
            "</form>"
        )

    reading_review_rows = "".join(
        f"<tr><td>{escape(str(r['read_date']))}</td><td>{escape(str(r['child_name']))}</td><td>{escape(str(r['book_title']))}</td><td>{escape(str(r['chapters']))}</td><td><span class='truncate' title=\"{escape(str(r.get('question_1') or '-'))}\">{escape(str(r.get('question_1') or '-'))}</span></td><td><span class='truncate' title=\"{escape(str(r.get('answer_1') or '-'))}\">{escape(str(r.get('answer_1') or '-'))}</span></td><td><span class='truncate' title=\"{escape(str(r.get('question_2') or '-'))}\">{escape(str(r.get('question_2') or '-'))}</span></td><td><span class='truncate' title=\"{escape(str(r.get('answer_2') or '-'))}\">{escape(str(r.get('answer_2') or '-'))}</span></td><td>{float(r.get('score') or 0):.2f}</td><td>{'yes' if int(r.get('passed') or 0) else 'no'}</td><td>{escape(str(r.get('credit_completion_id') or '-'))}</td><td>{escape(str(r.get('chatbot_provider') or '-'))}</td><td>{escape(str(r.get('parent_override_by') or '-'))}</td><td>{escape(str(r.get('status') or '-'))}</td><td>{_reading_action_cell(r)}</td></tr>"
        for r in reading_logs_all
    )
    reading_pending_count = sum(1 for r in reading_logs_all if str(r.get("status")) == "pending_questions")
    reading_awaiting_count = sum(1 for r in reading_logs_all if str(r.get("status")) == "awaiting_answers")
    reading_failed_count = sum(1 for r in reading_logs_all if str(r.get("status")) == "failed")
    reading_passed_count = sum(1 for r in reading_logs_all if str(r.get("status")) == "passed")
    reading_overridden_count = sum(1 for r in reading_logs_all if str(r.get("parent_override_by") or "").strip())
    interest_accrual_rows = "".join(
        f"<tr><td>{escape(str(i['accrual_date']))}</td><td>{escape(str(i['child_name']))}</td><td>${float(i['opening_balance']):.2f}</td><td>{float(i['rate_percent']):.4f}%</td><td>${float(i['interest_amount']):.2f}</td><td>{escape(str(i['created_at']))}</td></tr>"
        for i in interest_accruals
    )

    body = f"""
    <section class="two-col parent-panel">
      <div>
        <h2>Parent Panel</h2>
        {"<p class='msg'>Parent birthday today: " + ", ".join(escape(str(p["name"])) for p in parent_birthdays) + "</p>" if parent_birthdays else ""}
        <form method="post" action="/parent-logout"><button type="submit">Sign Out</button></form>
        <div class="card" id="parent-view-card">
          <h3>Panel View</h3>
          <p class="muted">Choose a menu view to focus on one area.</p>
          <select id="parent-view-select">
            <option value="all">All Sections</option>
            <option value="money">Money &amp; Rewards</option>
            <option value="reading">Reading</option>
            <option value="schedule">Schedule &amp; Activities</option>
            <option value="communication">Communication &amp; Alerts</option>
            <option value="content">Daily Content</option>
            <option value="pets">Pets</option>
            <option value="admin">Admin</option>
          </select>
        </div>
        <div class="card">
          <form method="get" action="/parent">
            <label>
              <input type="checkbox" name="show_inactive" value="1" {"checked" if include_inactive else ""} />
              Show inactive schedules
            </label>
            <button type="submit">Apply</button>
          </form>
          <form method="get" action="/export-data-json"><button type="submit">Export Data JSON</button></form>
          <form method="get" action="/export-ledger-csv"><button type="submit">Export Ledger CSV</button></form>
          <form method="get" action="/export-interest-csv">
            <input type="hidden" name="interest_child_id" value="{escape(interest_child_id)}" />
            <input type="hidden" name="interest_date_from" value="{escape(interest_from_value)}" />
            <input type="hidden" name="interest_date_to" value="{escape(interest_to_value)}" />
            <input type="hidden" name="interest_preset" value="{escape(preset)}" />
            <button type="submit">Export Interest CSV</button>
          </form>
          <form method="get" action="/export-reading-csv">
            <input type="hidden" name="reading_child_id" value="{escape(reading_child_id)}" />
            <input type="hidden" name="reading_status" value="{escape(reading_status_value)}" />
            <input type="hidden" name="reading_date_from" value="{escape(reading_date_from_value)}" />
            <input type="hidden" name="reading_date_to" value="{escape(reading_date_to_value)}" />
            <button type="submit">Export Reading CSV</button>
          </form>
        </div>
        <div class="card">
          <h3>Pending Reviews</h3>
          <table><thead><tr><th>ID</th><th>Child</th><th>Task</th><th>Due Date</th><th>Action</th></tr></thead>
          <tbody>{pending_rows if pending_rows else '<tr><td colspan="5">No pending reviews.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Task Schedules</h3>
          <table><thead><tr><th>ID</th><th>Task</th><th>Child</th><th>Cadence</th><th>Weekday</th><th>Due Time</th><th>Status</th><th>Actions</th></tr></thead>
          <tbody>{schedule_rows if schedule_rows else '<tr><td colspan="8">No schedules yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Balances</h3>
          <p class="muted">Daily interest rate: {wallet_interest_rate:.4f}% | Family interest earned: ${family_interest_total:.2f}</p>
          <form method="post" action="/update-interest-rate">
            <input type="number" name="rate_percent" min="0" step="0.0001" value="{wallet_interest_rate:.4f}" />
            <button type="submit">Set Daily Interest %</button>
          </form>
          <table><thead><tr><th>Child</th><th>Asset</th><th>Balance</th></tr></thead>
          <tbody>{balance_rows if balance_rows else '<tr><td colspan="3">No balances yet.</td></tr>'}</tbody></table>
          <h4>Interest History</h4>
          <form method="get" action="/parent">
            <input type="hidden" name="show_inactive" value="{1 if include_inactive else 0}" />
            <input type="hidden" name="concert_artist" value="{escape(concert_artist)}" />
            <input type="hidden" name="concert_city" value="{escape(concert_city)}" />
            <input type="hidden" name="concert_state" value="{escape(concert_state)}" />
            <select name="interest_child_id">
              <option value="">All children</option>
              {''.join(f'<option value="{c["id"]}" {"selected" if str(c["id"]) == interest_child_id else ""}>{escape(str(c["name"]))}</option>' for c in children)}
            </select>
            <input type="date" name="interest_date_from" value="{escape(interest_from_value)}" />
            <input type="date" name="interest_date_to" value="{escape(interest_to_value)}" />
            <button type="submit">Filter Interest</button>
            <button type="submit" name="interest_preset" value="last7">Last 7 Days</button>
            <button type="submit" name="interest_preset" value="this_month">This Month</button>
            <button type="submit" name="interest_preset" value="ytd">Year to Date</button>
          </form>
          <table><thead><tr><th>Date</th><th>Child</th><th>Opening Balance</th><th>Rate</th><th>Interest</th><th>Logged At</th></tr></thead>
          <tbody>{interest_accrual_rows if interest_accrual_rows else '<tr><td colspan="6">No interest accruals yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Charities</h3>
          <table><thead><tr><th>Name</th><th>Website</th><th>Site Live</th><th>Tax Exempt</th><th>Verified By</th><th>Added By</th><th>Actions</th></tr></thead>
          <tbody>{charity_rows if charity_rows else '<tr><td colspan="7">No charities yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Donation Requests</h3>
          <table><thead><tr><th>ID</th><th>Child</th><th>Charity</th><th>Amount</th><th>Status</th><th>Note</th><th>Action</th></tr></thead>
          <tbody>{donation_rows if donation_rows else '<tr><td colspan="7">No donation requests yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Wallet Payout Queue &amp; Audit</h3>
          <table><thead><tr><th>ID</th><th>Child</th><th>Amount</th><th>Status</th><th>Note</th><th>Sent By</th><th>Reference</th><th>Action</th></tr></thead>
          <tbody>{wallet_rows if wallet_rows else '<tr><td colspan="8">No payout requests yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Service Hours</h3>
          <p class="muted">Credit rates: ${service_rates['allowance_per_hour']:.2f}/hour allowance, {service_rates['screen_minutes_per_hour']:.0f} min/hour screen time.</p>
          <form method="post" action="/update-service-rates">
            <input type="number" min="0" step="0.01" name="allowance_per_hour" value="{service_rates['allowance_per_hour']:.2f}" />
            <input type="number" min="0" step="1" name="screen_minutes_per_hour" value="{service_rates['screen_minutes_per_hour']:.0f}" />
            <button type="submit">Update Service Rates</button>
          </form>
          <h4>Organizations</h4>
          <table><thead><tr><th>Name</th><th>Website</th><th>Added By</th></tr></thead>
          <tbody>{service_org_rows if service_org_rows else '<tr><td colspan="3">No service organizations yet.</td></tr>'}</tbody></table>
          <form method="post" action="/add-service-organization">
            <input type="hidden" name="sender_scope" value="parent" />
            <input name="name" placeholder="Add organization" required />
            <input name="website" placeholder="https://org.org" />
            <button type="submit">Add Organization</button>
          </form>
          <h4>Submitted Service Entries</h4>
          <table><thead><tr><th>ID</th><th>Child</th><th>Organization</th><th>Hours</th><th>Date</th><th>Status</th><th>Action</th></tr></thead>
          <tbody>{service_entry_rows if service_entry_rows else '<tr><td colspan="7">No service entries yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Family Schedule Defaults</h3>
          <p class="muted">Per child: Sun-Thu screen off + bedtime, and Fri-Sat screen off + bedtime.</p>
          <table><thead><tr><th>Child</th><th>Rules</th></tr></thead>
          <tbody>{house_rule_rows if house_rule_rows else '<tr><td colspan="2">No children configured.</td></tr>'}</tbody></table>
          <h4>After School Reminders</h4>
          <form method="post" action="/update-after-school-reminders">
            <input name="reminders" value="{escape(', '.join(reminders))}" placeholder="homework, practice voice, practice piano, exercise, read" style="min-width: 520px;" required />
            <button type="submit">Save Reminders</button>
          </form>
          <h4>Today's Child Activities</h4>
          <table><thead><tr><th>Child</th><th>Start</th><th>Activity</th><th>Type</th></tr></thead>
          <tbody>{''.join(activity_today_rows) if activity_today_rows else '<tr><td colspan="4">No activities scheduled today.</td></tr>'}</tbody></table>
          <h4>Holidays</h4>
          <table><thead><tr><th>Date</th><th>Name</th><th>No School</th></tr></thead>
          <tbody>{holiday_rows if holiday_rows else '<tr><td colspan="3">No holidays configured.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Message of the Day</h3>
          <p class="muted">Today: {escape(str(motd_today['message_text'])) if motd_today else 'No message set for today.'}</p>
          <form method="post" action="/add-motd-message">
            <input name="message_text" placeholder="Add message to library" style="min-width: 520px;" required />
            <button type="submit">Add To Library</button>
          </form>
          <form method="post" action="/set-motd">
            <input type="date" name="target_date" value="{date.today().isoformat()}" required />
            <input name="set_by" placeholder="Parent name" required />
            <select name="library_id">
              <option value="">Choose from library (optional)</option>
              {motd_library_options}
            </select>
            <input name="message_text" placeholder="Or type custom message for this date" style="min-width: 420px;" />
            <button type="submit">Set Message</button>
          </form>
          <p class="muted">Library size: {len(motd_library)} messages.</p>
        </div>

        <div class="card">
          <h3>Fun Fact of the Day</h3>
          <p class="muted">Today: {escape(str(fun_fact_today['fact_text'])) if fun_fact_today else 'No fun fact set for today.'}</p>
          <form method="post" action="/add-fun-fact">
            <input name="fact_text" placeholder="Add fun fact to library" style="min-width: 520px;" required />
            <button type="submit">Add To Library</button>
          </form>
          <form method="post" action="/set-fun-fact">
            <input type="date" name="target_date" value="{date.today().isoformat()}" required />
            <input name="set_by" placeholder="Parent name" required />
            <select name="library_id">
              <option value="">Choose from library (optional)</option>
              {fun_fact_library_options}
            </select>
            <input name="fact_text" placeholder="Or type custom fact for this date" style="min-width: 420px;" />
            <button type="submit">Set Fun Fact</button>
          </form>
          <p class="muted">Library size: {len(fun_fact_library)} facts.</p>
        </div>

        <div class="card">
          <h3>Messages</h3>
          <table><thead><tr><th>When</th><th>From</th><th>To</th><th>Message</th></tr></thead>
          <tbody>{message_rows if message_rows else '<tr><td colspan="4">No messages yet.</td></tr>'}</tbody></table>
          <form method="post" action="/post-message">
            <input type="hidden" name="sender_type" value="parent" />
            <input name="sender_name" placeholder="Parent name" required />
            <select name="child_id"><option value="">All children</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
            <input name="message_text" placeholder="Message for kids..." required />
            <button type="submit">Send Message</button>
          </form>
        </div>

        <div class="card">
          <h3>Pet Center</h3>
          <h4>Available Pets</h4>
          <table><thead><tr><th>Species</th><th>Rarity</th><th>Creator</th></tr></thead>
          <tbody>{pet_species_rows if pet_species_rows else '<tr><td colspan="3">No pets.</td></tr>'}</tbody></table>
          <form method="post" action="/create-pet-species">
            <input type="hidden" name="sender_scope" value="parent" />
            <input name="pet_name" placeholder="Add species" required />
            <select name="rarity">
              <option value="common">common</option>
              <option value="uncommon">uncommon</option>
              <option value="rare">rare</option>
              <option value="mythic">mythic</option>
              <option value="legendary">legendary</option>
            </select>
            <button type="submit">Create Species</button>
          </form>
          <h4>Adoptions</h4>
          <table><thead><tr><th>Week</th><th>Child</th><th>Pet Name</th><th>Species</th></tr></thead>
          <tbody>{pet_adoption_rows if pet_adoption_rows else '<tr><td colspan="4">No adoptions yet.</td></tr>'}</tbody></table>
          <h4>Badges</h4>
          <table><thead><tr><th>Week</th><th>Child</th><th>Badge</th></tr></thead>
          <tbody>{pet_badge_rows if pet_badge_rows else '<tr><td colspan="3">No badges yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Parents</h3>
          <table><thead><tr><th>Name</th><th>Email</th><th>Birthday</th></tr></thead>
          <tbody>{parent_rows if parent_rows else '<tr><td colspan="3">No parents configured.</td></tr>'}</tbody></table>
        </div>
      </div>

      <div>
        <div class="card">
          <h3>Set Child PIN</h3>
          <form method="post" action="/set-child-pin">
            <select name="child_id" required>
              <option value="">Child</option>
              {''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}
            </select>
            <input type="password" name="pin" placeholder="4-8 digits" required />
            <button type="submit">Save PIN</button>
          </form>
        </div>

        <div class="card">
          <h3>Child Contact &amp; PIN Reset</h3>
          <table>
            <thead><tr><th>Child</th><th>Email</th><th>Text Number</th><th>Actions</th></tr></thead>
            <tbody>
              {''.join(
                f"<tr><td>{escape(str(c['name']))}</td><td>{escape(str(c.get('email') or '-'))}</td><td>{escape(str(c.get('text_number') or '-'))}</td><td>"
                f"<form method='post' action='/update-child-contact'>"
                f"<input type='hidden' name='child_id' value='{c['id']}' />"
                f"<input name='email' value='{escape(str(c.get('email') or ''))}' placeholder='child@email.com' />"
                f"<input name='text_number' value='{escape(str(c.get('text_number') or ''))}' placeholder='text number' />"
                f"<button type='submit'>Save Contact</button></form>"
                f"<form method='post' action='/child-request-pin-reset'>"
                f"<input type='hidden' name='child_id' value='{c['id']}' />"
                f"<button type='submit'>Email PIN Reset Link</button></form>"
                f"</td></tr>"
                for c in children
              )}
            </tbody>
          </table>
        </div>

        <div class="card">
          <h3>Send Test Notification</h3>
          <form method="post" action="/send-test-notification">
            <select name="child_id" required>
              <option value="">Child</option>
              {''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}
            </select>
            <select name="channel" required>
              <option value="email">email</option>
              <option value="sms">sms</option>
              <option value="both">both</option>
            </select>
            <input name="message_text" placeholder="Optional custom test message" />
            <button type="submit">Send Test</button>
          </form>
          <p class="muted">Uses child email/text from contact settings.</p>
        </div>

        <div class="card">
          <h3>Add Task</h3>
          <form method="post" action="/add-task">
            <input name="name" placeholder="Task name" required />
            <select name="rank"><option value="required">required</option><option value="optional">optional</option></select>
            <select name="payout"><option value="allowance">allowance</option><option value="screen_time">screen_time</option><option value="points">points</option></select>
            <input type="number" name="value" step="0.01" min="0" required />
            <button type="submit">Add</button>
          </form>
        </div>

        <div class="card">
          <h3>Add Reward</h3>
          <form method="post" action="/add-reward">
            <input name="name" placeholder="Reward name" required />
            <select name="type"><option value="allowance">allowance</option><option value="screen_time">screen_time</option><option value="privilege">privilege</option></select>
            <input type="number" name="cost" step="0.01" min="0" required />
            <button type="submit">Add</button>
          </form>
        </div>

        <div class="card">
          <h3>Add Recurring Schedule</h3>
          <form method="post" action="/add-schedule">
            <select name="task_id" required>
              <option value="">Task</option>
              {''.join(f'<option value="{t["id"]}">{escape(str(t["name"]))}</option>' for t in tasks)}
            </select>
            <select name="child_id"><option value="">All children</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
            <select name="cadence"><option value="daily">daily</option><option value="weekly">weekly</option></select>
            <input type="number" name="day_of_week" min="0" max="6" placeholder="weekday 0-6" />
            <input name="due_time" placeholder="HH:MM" />
            <button type="submit">Add Schedule</button>
          </form>
          <form method="post" action="/generate-instances"><button type="submit">Generate Next 14 Days</button></form>
        </div>

        <div class="card">
          <h3>Adjust Balance</h3>
          <form method="post" action="/adjust-balance">
            <select name="child_id" required><option value="">Child</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
            <select name="asset"><option value="allowance">allowance</option><option value="screen_time">screen_time</option><option value="points">points</option></select>
            <input type="number" name="amount" step="0.01" required />
            <input name="note" placeholder="Reason" />
            <button type="submit">Adjust</button>
          </form>
        </div>

        <div class="card">
          <h3>Add Child Activity</h3>
          <form method="post" action="/add-child-activity">
            <select name="child_id" required><option value="">Child</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
            <input name="activity_name" placeholder="Activity name" required />
            <select name="category">
              <option value="activity">activity</option>
              <option value="school">school</option>
              <option value="sports">sports</option>
              <option value="lesson">lesson</option>
            </select>
            <input type="number" name="day_of_week" min="0" max="6" placeholder="weekday 0-6" />
            <input type="date" name="specific_date" />
            <input name="start_time" placeholder="HH:MM" required />
            <input name="end_time" placeholder="HH:MM optional" />
            <label><input type="checkbox" name="notify_enabled" value="1" /> notify</label>
            <input type="number" name="notify_minutes_before" min="0" max="1440" value="30" />
            <select name="notify_channels">
              <option value="email">email</option>
              <option value="sms">sms</option>
              <option value="both">both</option>
            </select>
            <input name="location" placeholder="Location optional" />
            <button type="submit">Add Activity</button>
          </form>
          <p class="muted">Use weekday for recurring events, or set a specific date for one-time events.</p>
          <h4>Notification Settings</h4>
          <table><thead><tr><th>Child</th><th>Activity</th><th>When</th><th>Start</th><th>Notify</th></tr></thead>
          <tbody>{activity_config_rows if activity_config_rows else '<tr><td colspan="5">No activities configured.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>App Time Limits</h3>
          <form method="post" action="/upsert-child-app-limit">
            <select name="child_id" required><option value="">Child</option>{''.join(f'<option value="{c["id"]}">{escape(str(c["name"]))}</option>' for c in children)}</select>
            <input name="app_name" placeholder="App name (ex: Roblox)" required />
            <input type="number" name="minutes_per_day" min="0" step="1" required />
            <label><input type="checkbox" name="active" value="1" checked /> active</label>
            <button type="submit">Save App Limit</button>
          </form>
          <table><thead><tr><th>Child</th><th>App</th><th>Minutes/Day</th><th>Status</th></tr></thead>
          <tbody>{app_limit_manage_rows if app_limit_manage_rows else '<tr><td colspan="4">No app limits configured.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Concert Rewards (80/20)</h3>
          <form method="get" action="/parent">
            <input type="hidden" name="show_inactive" value="{1 if include_inactive else 0}" />
            <input name="concert_artist" value="{escape(concert_artist)}" placeholder="Artist (Sabrina Carpenter, Benson Boone, Taylor Swift)" required />
            <input name="concert_city" value="{escape(concert_city)}" placeholder="City optional" />
            <input name="concert_state" value="{escape(concert_state)}" placeholder="State code optional" />
            <button type="submit">Search Ticketmaster</button>
          </form>
          {"<p class='muted'>Search error: " + escape(concert_search_error) + "</p>" if concert_search_error else ""}
          <table><thead><tr><th>Artist</th><th>Event</th><th>Date</th><th>Venue</th><th>City</th><th>State</th><th>Low</th><th>High</th><th>Action</th></tr></thead>
          <tbody>{concert_search_rows if concert_search_rows else '<tr><td colspan="9">No search results yet.</td></tr>'}</tbody></table>
          <h4>Concert Goals</h4>
          <table><thead><tr><th>Child</th><th>Artist</th><th>Event</th><th>Date</th><th>Range</th><th>Kid 80%</th><th>Parent 20%</th><th>Status</th><th>Action</th></tr></thead>
          <tbody>{concert_goal_rows if concert_goal_rows else '<tr><td colspan="9">No concert goals added yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Adventure Parks (80/20)</h3>
          <p class="muted">Seeded Southern California and Eastern Pennsylvania parks are listed below.</p>
          <table><thead><tr><th>Park</th><th>Region</th><th>Type</th><th>Range</th><th>Website</th><th>Add Goal</th></tr></thead>
          <tbody>{adventure_catalog_rows if adventure_catalog_rows else '<tr><td colspan="6">No adventure parks in catalog.</td></tr>'}</tbody></table>
          <form method="post" action="/add-adventure-park">
            <input name="name" placeholder="Park name" required />
            <input name="region" placeholder="Region (SoCal / Eastern PA)" required />
            <select name="category">
              <option value="theme_park">theme_park</option>
              <option value="water_park">water_park</option>
              <option value="adventure_park">adventure_park</option>
            </select>
            <input name="website" placeholder="https://park.example" />
            <input type="number" name="low_price" min="0" step="0.01" placeholder="Low price" required />
            <input type="number" name="high_price" min="0" step="0.01" placeholder="High price" required />
            <button type="submit">Add Park</button>
          </form>
          <h4>Adventure Goals</h4>
          <table><thead><tr><th>Child</th><th>Park</th><th>Ticket</th><th>Date</th><th>Region</th><th>Range</th><th>Kid 80%</th><th>Parent 20%</th><th>Status</th><th>Action</th></tr></thead>
          <tbody>{adventure_goal_rows if adventure_goal_rows else '<tr><td colspan="10">No adventure goals added yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Add Holiday</h3>
          <form method="post" action="/add-holiday">
            <input type="date" name="holiday_date" required />
            <input name="holiday_name" placeholder="Holiday name" required />
            <label><input type="checkbox" name="no_school" value="1" checked /> No school</label>
            <button type="submit">Save Holiday</button>
          </form>
        </div>

        <div class="card">
          <h3>iOS Usage Reports</h3>
          <table><thead><tr><th>Date</th><th>Child</th><th>Total Min</th><th>Per-App</th><th>Synced At</th></tr></thead>
          <tbody>{ios_report_rows if ios_report_rows else '<tr><td colspan="5">No iOS usage reports yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Reading Review</h3>
          <p class="muted">Pending: {reading_pending_count} | Awaiting answers: {reading_awaiting_count} | Failed: {reading_failed_count} | Passed: {reading_passed_count} | Overridden: {reading_overridden_count}</p>
          <form method="get" action="/parent">
            <input type="hidden" name="show_inactive" value="{1 if include_inactive else 0}" />
            <input type="hidden" name="concert_artist" value="{escape(concert_artist)}" />
            <input type="hidden" name="concert_city" value="{escape(concert_city)}" />
            <input type="hidden" name="concert_state" value="{escape(concert_state)}" />
            <input type="hidden" name="interest_child_id" value="{escape(interest_child_id)}" />
            <input type="hidden" name="interest_date_from" value="{escape(interest_from_value)}" />
            <input type="hidden" name="interest_date_to" value="{escape(interest_to_value)}" />
            <input type="hidden" name="interest_preset" value="{escape(preset)}" />
            <select name="reading_child_id">
              <option value="">All children</option>
              {''.join(f'<option value="{c["id"]}" {"selected" if str(c["id"]) == reading_child_id else ""}>{escape(str(c["name"]))}</option>' for c in children)}
            </select>
            <select name="reading_status">
              <option value="" {"selected" if reading_status_value == "" else ""}>all statuses</option>
              <option value="pending_questions" {"selected" if reading_status_value == "pending_questions" else ""}>pending_questions</option>
              <option value="awaiting_answers" {"selected" if reading_status_value == "awaiting_answers" else ""}>awaiting_answers</option>
              <option value="failed" {"selected" if reading_status_value == "failed" else ""}>failed</option>
              <option value="passed" {"selected" if reading_status_value == "passed" else ""}>passed</option>
            </select>
            <input type="date" name="reading_date_from" value="{escape(reading_date_from_value)}" />
            <input type="date" name="reading_date_to" value="{escape(reading_date_to_value)}" />
            <button type="submit">Filter Reading</button>
          </form>
          <table><thead><tr><th>Date</th><th>Child</th><th>Book</th><th>Chapters</th><th>Q1</th><th>A1</th><th>Q2</th><th>A2</th><th>Score</th><th>Passed</th><th>Credit ID</th><th>Provider</th><th>Override By</th><th>Status</th><th>Action</th></tr></thead>
          <tbody>{reading_review_rows if reading_review_rows else '<tr><td colspan="15">No reading logs yet.</td></tr>'}</tbody></table>
        </div>

        <div class="card">
          <h3>Notification Delivery Log</h3>
          <table><thead><tr><th>When</th><th>Child</th><th>Activity</th><th>Channel</th><th>Target</th><th>Status</th><th>Error</th></tr></thead>
          <tbody>{notification_attempt_rows if notification_attempt_rows else '<tr><td colspan="7">No notification attempts yet.</td></tr>'}</tbody></table>
        </div>
      </div>
    </section>
    <script>
      (function() {{
        const select = document.getElementById("parent-view-select");
        const panel = document.querySelector(".parent-panel");
        if (!select || !panel) return;
        const cards = Array.from(panel.querySelectorAll(".card")).filter((card) => card.id !== "parent-view-card");
        const classify = (card) => {{
          const h3 = card.querySelector("h3");
          const title = (h3 ? h3.textContent : "").toLowerCase();
          if (title.includes("reading review")) return "reading";
          if (title.includes("schedule") || title.includes("activity") || title.includes("holiday")) return "schedule";
          if (title.includes("notification") || title.includes("ios usage") || title.includes("contact settings")) return "communication";
          if (title.includes("message of the day") || title.includes("fun fact") || title === "messages") return "content";
          if (title.includes("pet")) return "pets";
          if (
            title.includes("wallet") ||
            title.includes("balance") ||
            title.includes("reward") ||
            title.includes("concert") ||
            title.includes("adventure") ||
            title.includes("charit") ||
            title.includes("donation") ||
            title.includes("service hours") ||
            title.includes("app time") ||
            title.includes("add task") ||
            title.includes("add reward") ||
            title.includes("adjust balance")
          ) return "money";
          return "admin";
        }};
        const apply = (view) => {{
          cards.forEach((card) => {{
            const section = classify(card);
            card.style.display = (view === "all" || view === section) ? "" : "none";
          }});
          try {{ localStorage.setItem("parent-panel-view", view); }} catch (err) {{}}
        }};
        let initial = "all";
        try {{
          const saved = localStorage.getItem("parent-panel-view");
          if (saved) initial = saved;
        }} catch (err) {{}}
        select.value = initial;
        apply(initial);
        select.addEventListener("change", () => apply(select.value));
      }})();
    </script>
    """
    return _html_response(_layout("Parent", body, msg))


def create_app() -> Callable:
    def app(environ: Environ, start_response: StartResponse):
        method = str(environ.get("REQUEST_METHOD") or "GET").upper()
        path = str(environ.get("PATH_INFO") or "/")
        host = str(environ.get("HTTP_HOST") or "127.0.0.1:8000")
        scheme = str(environ.get("wsgi.url_scheme") or "http")
        host_url = f"{scheme}://{host}"
        form: dict[str, str] = {}
        raw_body = b""
        if method == "POST":
            raw_length = environ.get("CONTENT_LENGTH") or "0"
            try:
                length = int(raw_length)
            except ValueError:
                length = 0
            raw_body = environ["wsgi.input"].read(length) if length > 0 else b""
            json_api_paths = {"/api/v1/ios/usage-sync", "/api/v1/ios/safety-alert"}
            if path not in json_api_paths:
                parsed = parse_qs(raw_body.decode("utf-8"), keep_blank_values=True)
                form = {k: v[0] for k, v in parsed.items()}

        try:
            # Keep due tasks generated ahead for child portals.
            today = date.today()
            apply_daily_wallet_interest(today)
            generate_task_instances(today, today + timedelta(days=13))
            for child in list_children():
                cid = int(child["id"])
                generate_pet_help_messages(cid)
                apply_birthday_treatment(cid, today)
            for parent in list_parents(active_only=True):
                apply_parent_birthday_treatment(int(parent["id"]), today)
            try:
                _process_activity_notifications()
            except Exception:
                pass

            parent_protected_paths = {
                "/review-completion",
                "/set-child-pin",
                "/add-task",
                "/add-reward",
                "/add-schedule",
                "/update-schedule",
                "/toggle-schedule-active",
                "/delete-schedule",
                "/generate-instances",
                "/adjust-balance",
                "/verify-charity",
                "/complete-donation",
                "/recheck-charity-site",
                "/mark-wallet-payout-sent",
                "/update-service-rates",
                "/update-interest-rate",
                "/review-service-hours",
                "/update-child-house-rules",
                "/update-after-school-reminders",
                "/add-motd-message",
                "/set-motd",
                "/add-fun-fact",
                "/set-fun-fact",
                "/add-child-activity",
                "/add-holiday",
                "/update-child-contact",
                "/child-request-pin-reset",
                "/update-activity-notify",
                "/send-test-notification",
                "/upsert-child-app-limit",
                "/add-concert-goal",
                "/set-concert-goal-status",
                "/add-adventure-park",
                "/add-adventure-goal",
                "/set-adventure-goal-status",
                "/override-reading-credit",
            }
            if method == "POST" and path in parent_protected_paths and not _is_parent_authed(environ):
                raise ValueError("Parent authentication required")
            if method == "GET" and path in ("/export-data-json", "/export-ledger-csv", "/export-interest-csv", "/export-reading-csv") and not _is_parent_authed(environ):
                raise ValueError("Parent authentication required")

            if method == "GET" and path == "/":
                result = _home_page(msg=_query_value(environ, "msg"))
            elif method == "POST" and path == "/api/v1/ios/usage-sync":
                if not _require_ios_bearer_if_configured(environ):
                    result = _json_response({"ok": False, "error": "Unauthorized"}, status="401 Unauthorized")
                    status, headers, body = result
                    start_response(status, headers)
                    return [body]
                try:
                    payload = json.loads(raw_body.decode("utf-8") if raw_body else "{}")
                except json.JSONDecodeError as err:
                    raise ValueError(f"Invalid JSON payload: {err}") from err
                child_name = str(payload.get("childName") or "").strip()
                usage_date = str(payload.get("date") or "").strip()
                total_minutes = int(payload.get("totalMinutes") or 0)
                per_app = payload.get("perAppMinutes") or {}
                if not isinstance(per_app, dict):
                    raise ValueError("perAppMinutes must be an object")
                report_id = save_ios_usage_report(
                    child_name=child_name,
                    usage_date=usage_date,
                    total_minutes=total_minutes,
                    per_app_minutes={str(k): int(v) for k, v in per_app.items()},
                )
                result = _json_response({"ok": True, "reportId": report_id}, status="200 OK")
            elif method == "POST" and path == "/api/v1/ios/safety-alert":
                if not _require_ios_bearer_if_configured(environ):
                    result = _json_response({"ok": False, "error": "Unauthorized"}, status="401 Unauthorized")
                    status, headers, body = result
                    start_response(status, headers)
                    return [body]
                try:
                    payload = json.loads(raw_body.decode("utf-8") if raw_body else "{}")
                except json.JSONDecodeError as err:
                    raise ValueError(f"Invalid JSON payload: {err}") from err

                child_name = str(payload.get("childName") or "").strip()
                alert_date = str(payload.get("date") or "").strip()
                severity = str(payload.get("severity") or "warning").strip().lower()
                reason = str(payload.get("reason") or "").strip()
                notify_email = _as_bool(payload.get("notifyParentsEmail"), default=True)
                notify_sms = _as_bool(payload.get("notifyParentsSms"), default=True)
                if not child_name:
                    raise ValueError("childName is required")
                if not alert_date:
                    raise ValueError("date is required")
                if severity not in {"info", "warning", "critical"}:
                    raise ValueError("severity must be info, warning, or critical")
                if not reason:
                    raise ValueError("reason is required")

                child_id: int | None = None
                for child in list_children(active_only=False):
                    if str(child.get("name") or "").strip().lower() == child_name.lower():
                        child_id = int(child["id"])
                        break

                message_text = f"[iOS Safety {severity.upper()}] {reason}"
                message_id = add_message(
                    sender_type="system",
                    sender_name="Safety Center",
                    child_id=child_id,
                    message_kind=f"ios_safety_{severity}",
                    message_text=message_text,
                )

                email_sent = 0
                sms_sent = 0
                errors: list[str] = []
                parents = list_parents(active_only=True)
                if notify_email:
                    for parent in parents:
                        to_email = str(parent.get("email") or "").strip()
                        if not to_email:
                            continue
                        try:
                            _send_plain_email(
                                to_email=to_email,
                                subject=f"Safety Alert: {child_name} ({severity.upper()})",
                                content=(
                                    f"Child: {child_name}\n"
                                    f"Date: {alert_date}\n"
                                    f"Severity: {severity.upper()}\n"
                                    f"Reason: {reason}\n"
                                ),
                            )
                            email_sent += 1
                        except Exception as err:  # noqa: BLE001
                            errors.append(f"email:{to_email}:{err}")
                if notify_sms:
                    for parent in parents:
                        to_number = str(parent.get("text_number") or "").strip()
                        if not to_number:
                            continue
                        try:
                            _send_apeiron_sms(
                                to_number,
                                f"Safety alert for {child_name} ({severity.upper()}): {reason}",
                            )
                            sms_sent += 1
                        except Exception as err:  # noqa: BLE001
                            errors.append(f"sms:{to_number}:{err}")

                result = _json_response(
                    {
                        "ok": True,
                        "messageId": message_id,
                        "emailSent": email_sent,
                        "smsSent": sms_sent,
                        "errors": errors[:5],
                    },
                    status="200 OK",
                )
            elif method == "POST" and path == "/api/v1/reading-chatbot":
                expected = os.environ.get("READING_CHATBOT_API_KEY", "").strip()
                auth_header = str(environ.get("HTTP_AUTHORIZATION") or "")
                if expected and auth_header != f"Bearer {expected}":
                    result = _json_response({"ok": False, "error": "Unauthorized"}, status="401 Unauthorized")
                    status, headers, body = result
                    start_response(status, headers)
                    return [body]
                try:
                    payload = json.loads(raw_body.decode("utf-8") if raw_body else "{}")
                except json.JSONDecodeError as err:
                    raise ValueError(f"Invalid JSON payload: {err}") from err
                result = _json_response(_reading_adapter_response(payload), status="200 OK")
            elif method == "GET" and path == "/export-data-json":
                payload = {
                    "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "children": list_children(),
                    "balances": list_balances(),
                    "wallet_daily_interest_rate_percent": get_wallet_daily_interest_rate_percent(),
                    "total_interest_earned": total_interest_earned(),
                    "ledger": list_ledger_entries(),
                    "service_entries": list_service_entries(limit=1000),
                    "ios_usage_reports": list_ios_usage_reports(limit=1000),
                    "concert_goals": list_concert_goals(child_id=None, active_only=False),
                    "adventure_goals": list_adventure_goals(child_id=None, active_only=False),
                }
                result = _json_response(payload, status="200 OK")
            elif method == "GET" and path == "/export-ledger-csv":
                rows = list_ledger_entries()
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(["id", "child_name", "asset_type", "amount", "source_type", "task_name", "reward_name", "note", "created_at"])
                for row in rows:
                    writer.writerow(
                        [
                            row.get("id"),
                            row.get("child_name"),
                            row.get("asset_type"),
                            row.get("amount"),
                            row.get("source_type"),
                            row.get("task_name"),
                            row.get("reward_name"),
                            row.get("note"),
                            row.get("created_at"),
                        ]
                    )
                result = (
                    "200 OK",
                    [
                        ("Content-Type", "text/csv; charset=utf-8"),
                        ("Content-Disposition", 'attachment; filename="ledger_export.csv"'),
                    ],
                    output.getvalue().encode("utf-8"),
                )
            elif method == "GET" and path == "/export-interest-csv":
                interest_child_id = _query_value(environ, "interest_child_id", "").strip()
                interest_date_from = _query_value(environ, "interest_date_from", "").strip()
                interest_date_to = _query_value(environ, "interest_date_to", "").strip()
                interest_preset = _query_value(environ, "interest_preset", "").strip().lower()
                if interest_preset in {"last7", "this_month", "ytd"}:
                    today = date.today()
                    if interest_preset == "last7":
                        interest_date_from = (today - timedelta(days=6)).isoformat()
                        interest_date_to = today.isoformat()
                    elif interest_preset == "this_month":
                        interest_date_from = date(today.year, today.month, 1).isoformat()
                        interest_date_to = today.isoformat()
                    elif interest_preset == "ytd":
                        interest_date_from = date(today.year, 1, 1).isoformat()
                        interest_date_to = today.isoformat()
                child_id = int(interest_child_id) if interest_child_id.isdigit() else None
                rows = list_interest_accruals(
                    child_id=child_id,
                    date_from=interest_date_from or None,
                    date_to=interest_date_to or None,
                    limit=5000,
                )
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(["id", "accrual_date", "child_name", "opening_balance", "rate_percent", "interest_amount", "created_at"])
                for row in rows:
                    writer.writerow(
                        [
                            row.get("id"),
                            row.get("accrual_date"),
                            row.get("child_name"),
                            row.get("opening_balance"),
                            row.get("rate_percent"),
                            row.get("interest_amount"),
                            row.get("created_at"),
                        ]
                    )
                result = (
                    "200 OK",
                    [
                        ("Content-Type", "text/csv; charset=utf-8"),
                        ("Content-Disposition", 'attachment; filename="interest_history.csv"'),
                    ],
                    output.getvalue().encode("utf-8"),
                )
            elif method == "GET" and path == "/export-reading-csv":
                reading_child_id = _query_value(environ, "reading_child_id", "").strip()
                reading_status = _query_value(environ, "reading_status", "").strip()
                reading_date_from = _query_value(environ, "reading_date_from", "").strip()
                reading_date_to = _query_value(environ, "reading_date_to", "").strip()
                child_id = int(reading_child_id) if reading_child_id.isdigit() else None
                rows = list_reading_logs(
                    child_id=child_id,
                    status=reading_status or None,
                    date_from=reading_date_from or None,
                    date_to=reading_date_to or None,
                    limit=5000,
                )
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(
                    [
                        "id",
                        "read_date",
                        "child_name",
                        "start_time",
                        "end_time",
                        "book_title",
                        "chapters",
                        "question_1",
                        "answer_1",
                        "question_2",
                        "answer_2",
                        "score",
                        "passed",
                        "status",
                        "credit_completion_id",
                        "chatbot_provider",
                        "parent_override_by",
                        "parent_override_at",
                        "created_at",
                        "evaluated_at",
                    ]
                )
                for row in rows:
                    writer.writerow(
                        [
                            row.get("id"),
                            row.get("read_date"),
                            row.get("child_name"),
                            row.get("start_time"),
                            row.get("end_time"),
                            row.get("book_title"),
                            row.get("chapters"),
                            row.get("question_1"),
                            row.get("answer_1"),
                            row.get("question_2"),
                            row.get("answer_2"),
                            row.get("score"),
                            row.get("passed"),
                            row.get("status"),
                            row.get("credit_completion_id"),
                            row.get("chatbot_provider"),
                            row.get("parent_override_by"),
                            row.get("parent_override_at"),
                            row.get("created_at"),
                            row.get("evaluated_at"),
                        ]
                    )
                result = (
                    "200 OK",
                    [
                        ("Content-Type", "text/csv; charset=utf-8"),
                        ("Content-Disposition", 'attachment; filename="reading_review.csv"'),
                    ],
                    output.getvalue().encode("utf-8"),
                )
            elif method == "GET" and path == "/parent":
                if not _is_parent_authed(environ):
                    result = _parent_login_page(msg=_query_value(environ, "msg"))
                else:
                    include_inactive = _query_value(environ, "show_inactive", "1") == "1"
                    result = _parent_page(
                        msg=_query_value(environ, "msg"),
                        include_inactive=include_inactive,
                        concert_artist=_query_value(environ, "concert_artist"),
                        concert_city=_query_value(environ, "concert_city"),
                        concert_state=_query_value(environ, "concert_state"),
                        interest_child_id=_query_value(environ, "interest_child_id"),
                        interest_date_from=_query_value(environ, "interest_date_from"),
                        interest_date_to=_query_value(environ, "interest_date_to"),
                        interest_preset=_query_value(environ, "interest_preset"),
                        reading_child_id=_query_value(environ, "reading_child_id"),
                        reading_status=_query_value(environ, "reading_status"),
                        reading_date_from=_query_value(environ, "reading_date_from"),
                        reading_date_to=_query_value(environ, "reading_date_to"),
                    )
            elif method == "GET" and path == "/parent-login":
                result = _parent_login_page(msg=_query_value(environ, "msg"))
            elif method == "GET" and path == "/parent-reset":
                token = _query_value(environ, "token")
                if not token:
                    result = _redirect("/parent-login?msg=Missing+reset+token")
                else:
                    result = _parent_reset_page(
                        token=token,
                        msg=_query_value(environ, "msg"),
                    )
            elif method == "GET" and path == "/child-pin-reset":
                token = _query_value(environ, "token")
                if not token:
                    result = _redirect("/parent?msg=Missing+child+reset+token")
                else:
                    result = _child_pin_reset_page(
                        token=token,
                        msg=_query_value(environ, "msg"),
                    )
            elif method == "GET" and path == "/child":
                child_id = int(_query_value(environ, "child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    result = _child_login_page(child_id, msg=_query_value(environ, "msg"))
                else:
                    result = _child_page(child_id, msg=_query_value(environ, "msg"))
            elif method == "POST" and path == "/parent-login":
                if not verify_parent_password(form.get("password", "")):
                    result = _redirect("/parent-login?msg=Invalid+password")
                else:
                    result = _redirect(
                        "/parent?msg=Signed+in",
                        set_cookie=f"{PARENT_COOKIE_NAME}={_sign_parent()}; Path=/; HttpOnly; SameSite=Lax",
                    )
            elif method == "POST" and path == "/parent-logout":
                result = _redirect(
                    "/parent-login?msg=Signed+out",
                    set_cookie=f"{PARENT_COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax",
                )
            elif method == "POST" and path == "/parent-request-reset":
                sent = _send_parent_reset_emails(host_url, valid_minutes=30)
                result = _redirect(f"/parent-login?{urlencode({'msg': f'Reset email sent to {sent} parent account(s)'})}")
            elif method == "POST" and path == "/parent-request-reset-text":
                sent = _send_parent_reset_texts(host_url, valid_minutes=30)
                result = _redirect(f"/parent-login?{urlencode({'msg': f'Reset text sent to {sent} parent account(s)'})}")
            elif method == "POST" and path == "/parent-reset":
                ok = consume_parent_reset_token(
                    token=form.get("token", ""),
                    new_password=form.get("new_password", ""),
                )
                if not ok:
                    raise ValueError("Invalid or expired reset token")
                result = _redirect("/parent-login?msg=Password+updated")
            elif method == "POST" and path == "/child-pin-reset":
                ok = consume_child_pin_reset_token(
                    token=form.get("token", ""),
                    new_pin=form.get("new_pin", ""),
                )
                if not ok:
                    raise ValueError("Invalid or expired child reset token")
                result = _redirect("/parent?msg=Child+PIN+updated")
            elif method == "POST" and path == "/child-login":
                child_id = int(form.get("child_id", "0"))
                if not verify_child_pin(child_id, form.get("pin", "")):
                    result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Invalid PIN'})}")
                else:
                    token = f"{child_id}:{_sign_child(child_id)}"
                    result = _redirect(
                        f"/child?{urlencode({'child_id': child_id, 'msg': 'Welcome!'})}",
                        set_cookie=f"{COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Lax",
                    )
            elif method == "POST" and path == "/child-logout":
                child_id = int(form.get("child_id", "0"))
                result = _redirect(
                    f"/child?{urlencode({'child_id': child_id, 'msg': 'Portal locked'})}",
                    set_cookie=f"{COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax",
                )
            elif method == "POST" and path == "/submit-instance":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                submit_task_instance(int(form.get("instance_id", "0")), form.get("note", ""))
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Task submitted for review'})}")
            elif method == "POST" and path == "/submit-reading-log":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                log_id = create_reading_log(
                    child_id=child_id,
                    read_date=form.get("read_date", ""),
                    start_time=form.get("start_time", ""),
                    end_time=form.get("end_time", ""),
                    book_title=form.get("book_title", ""),
                    chapters=form.get("chapters", ""),
                )
                log = get_reading_log(log_id)
                if not log:
                    raise ValueError("Failed to load reading log")
                q1, q2, provider = _generate_reading_quiz_questions(
                    book_title=str(log["book_title"]),
                    chapters=str(log["chapters"]),
                )
                set_reading_log_questions(log_id, q1, q2, chatbot_provider=provider)
                result = _redirect(
                    f"/child?{urlencode({'child_id': child_id, 'msg': 'Reading saved. Answer the 2 quiz questions to earn credit.'})}"
                )
            elif method == "POST" and path == "/answer-reading-quiz":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                log_id = int(form.get("log_id", "0"))
                log = get_reading_log(log_id)
                if not log or int(log["child_id"]) != child_id:
                    raise ValueError("Reading log not found")
                passed, score = _evaluate_reading_quiz_answers(
                    book_title=str(log["book_title"]),
                    chapters=str(log["chapters"]),
                    question_1=str(log.get("question_1") or ""),
                    question_2=str(log.get("question_2") or ""),
                    answer_1=form.get("answer_1", ""),
                    answer_2=form.get("answer_2", ""),
                )
                updated = update_reading_log_quiz_result(
                    log_id=log_id,
                    answer_1=form.get("answer_1", ""),
                    answer_2=form.get("answer_2", ""),
                    passed=passed,
                    score=score,
                )
                if not updated:
                    raise ValueError("Reading quiz could not be updated")
                if passed:
                    completion_id = award_reading_log_credit(log_id, reviewed_by="Reading Bot")
                    add_message(
                        sender_type="system",
                        sender_name="Reading Bot",
                        child_id=child_id,
                        message_kind=f"reading_credit_{log_id}",
                        message_text=f"Great job! Reading quiz passed and task credit awarded (completion #{completion_id}).",
                    )
                    result = _redirect(
                        f"/child?{urlencode({'child_id': child_id, 'msg': 'Quiz passed! Reading task credit awarded.'})}"
                    )
                else:
                    result = _redirect(
                        f"/child?{urlencode({'child_id': child_id, 'msg': 'Quiz not passed yet. Try again with more detail.'})}"
                    )
            elif method == "POST" and path == "/redeem-reward":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                redeem_reward(child_id=child_id, reward_id=int(form.get("reward_id", "0")), note=form.get("note", ""))
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Reward redeemed'})}")
            elif method == "POST" and path == "/submit-charity":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                add_charity(
                    name=form.get("charity_name", ""),
                    website=form.get("charity_website", ""),
                    created_by_child_id=child_id,
                    tax_exempt_verified=False,
                )
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Charity submitted for parent review'})}")
            elif method == "POST" and path == "/create-donation-pledge":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                pledge_id = create_donation_pledge(
                    child_id=child_id,
                    charity_id=int(form.get("charity_id", "0")),
                    amount=float(form.get("amount", "0")),
                    note=form.get("note", ""),
                )
                add_message(
                    sender_type="system",
                    sender_name="Donation Bot",
                    child_id=child_id,
                    message_kind=f"donation_request_{pledge_id}",
                    message_text=f"Donation request #{pledge_id} submitted for parent completion.",
                )
                result = _redirect(
                    f"/child?{urlencode({'child_id': child_id, 'msg': 'Donation request submitted for parent completion'})}"
                )
            elif method == "POST" and path == "/request-wallet-payout":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                payout_id = request_wallet_payout(
                    child_id=child_id,
                    amount=float(form.get("amount", "0")),
                    note=form.get("note", ""),
                )
                add_message(
                    sender_type="system",
                    sender_name="Wallet Bot",
                    child_id=child_id,
                    message_kind=f"wallet_payout_{payout_id}",
                    message_text=f"Wallet payout request #{payout_id} is pending parent transfer.",
                )
                result = _redirect(
                    f"/child?{urlencode({'child_id': child_id, 'msg': 'Wallet payout request submitted'})}"
                )
            elif method == "POST" and path == "/submit-service-hours":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                entry_id = submit_service_hours(
                    child_id=child_id,
                    organization_id=int(form.get("organization_id", "0")),
                    hours=float(form.get("hours", "0")),
                    service_date=form.get("service_date", "") or None,
                    note=form.get("note", ""),
                )
                add_message(
                    sender_type="system",
                    sender_name="Service Bot",
                    child_id=child_id,
                    message_kind=f"service_entry_{entry_id}",
                    message_text=f"Service entry #{entry_id} submitted for parent review.",
                )
                result = _redirect(
                    f"/child?{urlencode({'child_id': child_id, 'msg': 'Service entry submitted for parent review'})}"
                )
            elif method == "POST" and path == "/add-service-organization":
                sender_scope = form.get("sender_scope", "parent")
                if sender_scope == "kid":
                    child_id = int(form.get("child_id", "0"))
                    if not _is_child_authed(environ, child_id):
                        raise ValueError("Session expired. Please login again.")
                    add_service_organization(
                        name=form.get("name", ""),
                        website=form.get("website", ""),
                        created_by_child_id=child_id,
                    )
                    result = _redirect(
                        f"/child?{urlencode({'child_id': child_id, 'msg': 'Service organization added'})}"
                    )
                else:
                    if not _is_parent_authed(environ):
                        raise ValueError("Parent authentication required")
                    add_service_organization(
                        name=form.get("name", ""),
                        website=form.get("website", ""),
                        created_by_child_id=None,
                    )
                    result = _redirect("/parent?msg=Service+organization+added")
            elif method == "POST" and path == "/adopt-pet":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                adopt_weekly_pet(
                    child_id=child_id,
                    pet_species_id=int(form.get("pet_species_id", "0")),
                    pet_name=form.get("pet_name", ""),
                )
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Weekly pet adopted!'})}")
            elif method == "POST" and path == "/set-default-pet":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                set_default_pet(
                    child_id=child_id,
                    pet_species_id=int(form.get("pet_species_id", "0")),
                    pet_name=form.get("pet_name", ""),
                )
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Default pet updated'})}")
            elif method == "POST" and path == "/pet-care":
                child_id = int(form.get("child_id", "0"))
                if not _is_child_authed(environ, child_id):
                    raise ValueError("Session expired. Please login again.")
                complete_pet_care(child_id=child_id, care_type=form.get("care_type", ""))
                result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Pet care completed'})}")
            elif method == "POST" and path == "/create-pet-species":
                sender_scope = form.get("sender_scope", "parent")
                if sender_scope == "kid":
                    child_id = int(form.get("child_id", "0"))
                    if not _is_child_authed(environ, child_id):
                        raise ValueError("Session expired. Please login again.")
                    create_pet_species(
                        name=form.get("pet_name", ""),
                        rarity=form.get("rarity", "custom"),
                        created_by_child_id=child_id,
                    )
                    result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Pet species added!'})}")
                else:
                    create_pet_species(
                        name=form.get("pet_name", ""),
                        rarity=form.get("rarity", "custom"),
                        created_by_child_id=None,
                    )
                    result = _redirect("/parent?msg=Pet+species+created")
            elif method == "POST" and path == "/post-message":
                sender_type = form.get("sender_type", "parent")
                sender_name = form.get("sender_name", "Parent")
                child_id_raw = form.get("child_id", "")
                child_id = int(child_id_raw) if child_id_raw else None
                if sender_type == "kid":
                    if child_id is None or not _is_child_authed(environ, child_id):
                        raise ValueError("Session expired. Please login again.")
                else:
                    if not _is_parent_authed(environ):
                        raise ValueError("Parent authentication required")
                add_message(
                    sender_type=sender_type,
                    sender_name=sender_name,
                    child_id=child_id,
                    message_text=form.get("message_text", ""),
                )
                if sender_type == "kid":
                    result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': 'Message sent'})}")
                else:
                    result = _redirect("/parent?msg=Message+sent")
            elif method == "POST" and path == "/review-completion":
                updated = review_task_completion(
                    completion_id=int(form.get("id", "0")),
                    decision=form.get("decision", ""),
                    reviewed_by=form.get("by", ""),
                    review_note=form.get("note", ""),
                )
                if not updated:
                    raise ValueError("Completion not found or no longer pending")
                result = _redirect("/parent?msg=Review+saved")
            elif method == "POST" and path == "/set-child-pin":
                if not set_child_pin(int(form.get("child_id", "0")), form.get("pin", "")):
                    raise ValueError("Child not found")
                result = _redirect("/parent?msg=PIN+updated")
            elif method == "POST" and path == "/update-child-contact":
                updated = update_child_contact_info(
                    child_id=int(form.get("child_id", "0")),
                    email=form.get("email", ""),
                    text_number=form.get("text_number", ""),
                )
                if not updated:
                    raise ValueError("Child not found")
                result = _redirect("/parent?msg=Child+contact+updated")
            elif method == "POST" and path == "/child-request-pin-reset":
                child_id = int(form.get("child_id", "0"))
                token, email, child_name = create_child_pin_reset_token(child_id, valid_minutes=30)
                _send_child_pin_reset_email(to_email=email, child_name=child_name, token=token, host_url=host_url)
                result = _redirect("/parent?msg=Child+PIN+reset+email+sent")
            elif method == "POST" and path == "/send-test-notification":
                child_id = int(form.get("child_id", "0"))
                child = get_child(child_id)
                if not child:
                    raise ValueError("Child not found")
                channel = form.get("channel", "email").strip().lower()
                if channel not in ("email", "sms", "both"):
                    raise ValueError("Invalid channel")
                message_text = form.get("message_text", "").strip() or f"Test notification for {child['name']}"
                sent = 0
                if channel in ("email", "both"):
                    to_email = str(child.get("email") or "").strip()
                    if not to_email:
                        raise ValueError(f"{child['name']} does not have an email set")
                    _send_plain_email(
                        to_email=to_email,
                        subject=f"Test Notification for {child['name']}",
                        content=message_text,
                    )
                    sent += 1
                if channel in ("sms", "both"):
                    to_number = str(child.get("text_number") or "").strip()
                    if not to_number:
                        raise ValueError(f"{child['name']} does not have a text number set")
                    _send_apeiron_sms(to_number, message_text)
                    sent += 1
                result = _redirect(f"/parent?{urlencode({'msg': f'Test notification sent ({sent})'})}")
            elif method == "POST" and path == "/upsert-child-app-limit":
                upsert_child_app_limit(
                    child_id=int(form.get("child_id", "0")),
                    app_name=form.get("app_name", ""),
                    minutes_per_day=int(form.get("minutes_per_day", "0")),
                    active=form.get("active", "") == "1",
                )
                result = _redirect("/parent?msg=App+time+limit+saved")
            elif method == "POST" and path == "/add-concert-goal":
                add_concert_goal(
                    child_id=int(form.get("child_id", "0")),
                    artist_name=form.get("artist_name", ""),
                    event_name=form.get("event_name", ""),
                    event_date=form.get("event_date", ""),
                    venue_name=form.get("venue_name", ""),
                    city=form.get("city", ""),
                    state_code=form.get("state_code", ""),
                    low_price=float(form.get("low_price", "0")),
                    high_price=float(form.get("high_price", "0")),
                    currency=form.get("currency", "USD"),
                    ticket_url=form.get("ticket_url", ""),
                    kid_share_percent=80,
                )
                result = _redirect("/parent?msg=Concert+goal+added")
            elif method == "POST" and path == "/set-concert-goal-status":
                updated = set_concert_goal_status(
                    goal_id=int(form.get("goal_id", "0")),
                    status=form.get("goal_status", ""),
                )
                if not updated:
                    raise ValueError("Concert goal not found")
                result = _redirect("/parent?msg=Concert+goal+status+updated")
            elif method == "POST" and path == "/add-adventure-park":
                add_adventure_park_catalog(
                    name=form.get("name", ""),
                    region=form.get("region", ""),
                    category=form.get("category", "theme_park"),
                    website=form.get("website", ""),
                    low_price=float(form.get("low_price", "0")),
                    high_price=float(form.get("high_price", "0")),
                )
                result = _redirect("/parent?msg=Adventure+park+added")
            elif method == "POST" and path == "/add-adventure-goal":
                add_adventure_goal(
                    child_id=int(form.get("child_id", "0")),
                    park_name=form.get("park_name", ""),
                    ticket_name=form.get("ticket_name", "Day Pass"),
                    target_date=form.get("target_date", ""),
                    low_price=float(form.get("low_price", "0")),
                    high_price=float(form.get("high_price", "0")),
                    region=form.get("region", ""),
                    category=form.get("category", "theme_park"),
                    currency=form.get("currency", "USD"),
                    ticket_url=form.get("ticket_url", ""),
                    kid_share_percent=80,
                )
                result = _redirect("/parent?msg=Adventure+goal+added")
            elif method == "POST" and path == "/set-adventure-goal-status":
                updated = set_adventure_goal_status(
                    goal_id=int(form.get("goal_id", "0")),
                    status=form.get("goal_status", ""),
                )
                if not updated:
                    raise ValueError("Adventure goal not found")
                result = _redirect("/parent?msg=Adventure+goal+status+updated")
            elif method == "POST" and path == "/override-reading-credit":
                completion_id = parent_override_reading_credit(
                    log_id=int(form.get("log_id", "0")),
                    reviewed_by=form.get("by", ""),
                )
                result = _redirect(f"/parent?{urlencode({'msg': f'Reading credit awarded (completion #{completion_id})'})}")
            elif method == "POST" and path == "/add-task":
                add_task(
                    name=form.get("name", ""),
                    rank=form.get("rank", ""),
                    payout_type=form.get("payout", ""),
                    payout_value=float(form.get("value", "0")),
                )
                result = _redirect("/parent?msg=Task+added")
            elif method == "POST" and path == "/add-reward":
                add_reward(name=form.get("name", ""), reward_type=form.get("type", ""), cost=float(form.get("cost", "0")))
                result = _redirect("/parent?msg=Reward+added")
            elif method == "POST" and path == "/add-schedule":
                child_id_raw = form.get("child_id", "")
                day_raw = form.get("day_of_week", "")
                add_task_schedule(
                    task_id=int(form.get("task_id", "0")),
                    cadence=form.get("cadence", ""),
                    child_id=int(child_id_raw) if child_id_raw else None,
                    day_of_week=int(day_raw) if day_raw else None,
                    due_time=form.get("due_time", "") or None,
                )
                result = _redirect("/parent?msg=Schedule+added")
            elif method == "POST" and path == "/update-schedule":
                child_id_raw = form.get("child_id", "")
                day_raw = form.get("day_of_week", "")
                updated = update_task_schedule(
                    schedule_id=int(form.get("schedule_id", "0")),
                    task_id=int(form.get("task_id", "0")),
                    cadence=form.get("cadence", ""),
                    child_id=int(child_id_raw) if child_id_raw else None,
                    day_of_week=int(day_raw) if day_raw else None,
                    due_time=form.get("due_time", "") or None,
                )
                if not updated:
                    raise ValueError("Schedule not found")
                show_inactive = form.get("show_inactive", "1")
                result = _redirect(
                    f"/parent?{urlencode({'show_inactive': show_inactive, 'msg': 'Schedule updated'})}"
                )
            elif method == "POST" and path == "/toggle-schedule-active":
                updated = set_task_schedule_active(
                    schedule_id=int(form.get("schedule_id", "0")),
                    active=form.get("active", "1") == "1",
                )
                if not updated:
                    raise ValueError("Schedule not found")
                show_inactive = form.get("show_inactive", "1")
                result = _redirect(
                    f"/parent?{urlencode({'show_inactive': show_inactive, 'msg': 'Schedule status updated'})}"
                )
            elif method == "POST" and path == "/delete-schedule":
                deleted = delete_task_schedule(int(form.get("schedule_id", "0")))
                if not deleted:
                    raise ValueError("Schedule not found")
                show_inactive = form.get("show_inactive", "1")
                result = _redirect(
                    f"/parent?{urlencode({'show_inactive': show_inactive, 'msg': 'Schedule deleted'})}"
                )
            elif method == "POST" and path == "/generate-instances":
                today = date.today()
                created = generate_task_instances(today, today + timedelta(days=13))
                result = _redirect(f"/parent?{urlencode({'msg': f'Generated {created} instances'})}")
            elif method == "POST" and path == "/adjust-balance":
                add_manual_ledger_entry(
                    child_id=int(form.get("child_id", "0")),
                    asset_type=form.get("asset", ""),
                    amount=float(form.get("amount", "0")),
                    note=form.get("note", ""),
                )
                result = _redirect("/parent?msg=Balance+adjusted")
            elif method == "POST" and path == "/verify-charity":
                updated = set_charity_tax_exempt_verified(
                    charity_id=int(form.get("charity_id", "0")),
                    verified_by_parent=form.get("by", ""),
                )
                if not updated:
                    raise ValueError("Charity not found")
                result = _redirect("/parent?msg=Charity+tax-exempt+status+verified")
            elif method == "POST" and path == "/recheck-charity-site":
                live = recheck_charity_website(charity_id=int(form.get("charity_id", "0")))
                text = "Charity site is live" if live else "Charity site check failed"
                result = _redirect(f"/parent?{urlencode({'msg': text})}")
            elif method == "POST" and path == "/complete-donation":
                completed = complete_donation_pledge(
                    pledge_id=int(form.get("pledge_id", "0")),
                    completed_by=form.get("by", ""),
                )
                if not completed:
                    raise ValueError("Donation is already completed")
                result = _redirect("/parent?msg=Donation+marked+completed")
            elif method == "POST" and path == "/mark-wallet-payout-sent":
                sent = mark_wallet_payout_sent(
                    payout_id=int(form.get("payout_id", "0")),
                    sent_by=form.get("by", ""),
                    transfer_reference=form.get("reference", ""),
                )
                if not sent:
                    raise ValueError("Payout is already completed")
                result = _redirect("/parent?msg=Wallet+payout+marked+sent")
            elif method == "POST" and path == "/update-service-rates":
                set_service_credit_rates(
                    allowance_per_hour=float(form.get("allowance_per_hour", "0")),
                    screen_minutes_per_hour=float(form.get("screen_minutes_per_hour", "0")),
                )
                result = _redirect("/parent?msg=Service+rates+updated")
            elif method == "POST" and path == "/update-interest-rate":
                set_wallet_daily_interest_rate_percent(float(form.get("rate_percent", "0")))
                result = _redirect("/parent?msg=Daily+interest+rate+updated")
            elif method == "POST" and path == "/review-service-hours":
                updated = review_service_hours(
                    entry_id=int(form.get("entry_id", "0")),
                    decision=form.get("decision", ""),
                    reviewed_by=form.get("by", ""),
                )
                if not updated:
                    raise ValueError("Service entry already reviewed")
                result = _redirect("/parent?msg=Service+entry+reviewed")
            elif method == "POST" and path == "/update-child-house-rules":
                updated = upsert_child_house_rules(
                    child_id=int(form.get("child_id", "0")),
                    weekday_screen_off=form.get("weekday_screen_off", ""),
                    weekday_bedtime=form.get("weekday_bedtime", ""),
                    weekend_screen_off=form.get("weekend_screen_off", ""),
                    weekend_bedtime=form.get("weekend_bedtime", ""),
                )
                if not updated:
                    raise ValueError("Child not found")
                result = _redirect("/parent?msg=Child+house+rules+updated")
            elif method == "POST" and path == "/update-after-school-reminders":
                set_after_school_reminders(form.get("reminders", ""))
                result = _redirect("/parent?msg=After-school+reminders+updated")
            elif method == "POST" and path == "/add-motd-message":
                add_motd_library_message(form.get("message_text", ""), category="parent_custom")
                result = _redirect("/parent?msg=MOTD+library+message+added")
            elif method == "POST" and path == "/set-motd":
                library_raw = form.get("library_id", "").strip()
                set_message_of_the_day(
                    target_date=form.get("target_date", ""),
                    set_by=form.get("set_by", ""),
                    message_text=form.get("message_text", ""),
                    library_id=int(library_raw) if library_raw else None,
                )
                result = _redirect("/parent?msg=Message+of+the+day+saved")
            elif method == "POST" and path == "/add-fun-fact":
                add_fun_fact_library_fact(form.get("fact_text", ""), category="parent_custom")
                result = _redirect("/parent?msg=Fun+fact+added+to+library")
            elif method == "POST" and path == "/set-fun-fact":
                library_raw = form.get("library_id", "").strip()
                set_fun_fact_of_the_day(
                    target_date=form.get("target_date", ""),
                    set_by=form.get("set_by", ""),
                    fact_text=form.get("fact_text", ""),
                    library_id=int(library_raw) if library_raw else None,
                )
                result = _redirect("/parent?msg=Fun+fact+of+the+day+saved")
            elif method == "POST" and path == "/add-child-activity":
                day_raw = form.get("day_of_week", "").strip()
                specific_date = form.get("specific_date", "").strip() or None
                if not day_raw and not specific_date:
                    raise ValueError("Set either weekday or specific date")
                add_child_activity(
                    child_id=int(form.get("child_id", "0")),
                    activity_name=form.get("activity_name", ""),
                    category=form.get("category", "activity"),
                    day_of_week=int(day_raw) if day_raw else None,
                    specific_date=specific_date,
                    start_time=form.get("start_time", ""),
                    end_time=form.get("end_time", "").strip() or None,
                    location=form.get("location", ""),
                    notify_enabled=form.get("notify_enabled", "") == "1",
                    notify_minutes_before=int(form.get("notify_minutes_before", "30")),
                    notify_channels=form.get("notify_channels", "email"),
                )
                result = _redirect("/parent?msg=Child+activity+added")
            elif method == "POST" and path == "/update-activity-notify":
                updated = update_activity_notification_settings(
                    activity_id=int(form.get("activity_id", "0")),
                    notify_enabled=form.get("notify_enabled", "") == "1",
                    notify_minutes_before=int(form.get("notify_minutes_before", "30")),
                    notify_channels=form.get("notify_channels", "email"),
                )
                if not updated:
                    raise ValueError("Activity not found")
                result = _redirect("/parent?msg=Activity+notification+settings+updated")
            elif method == "POST" and path == "/add-holiday":
                add_holiday(
                    holiday_date=form.get("holiday_date", ""),
                    holiday_name=form.get("holiday_name", ""),
                    no_school=form.get("no_school", "") == "1",
                )
                result = _redirect("/parent?msg=Holiday+saved")
            else:
                result = _html_response(_layout("Not Found", "<h2>Route not found.</h2>"), status="404 Not Found")
        except Exception as err:  # noqa: BLE001
            if method == "POST":
                if path in ("/api/v1/ios/usage-sync", "/api/v1/ios/safety-alert"):
                    result = _json_response({"ok": False, "error": str(err)}, status="400 Bad Request")
                elif path == "/api/v1/reading-chatbot":
                    result = _json_response({"ok": False, "error": str(err)}, status="400 Bad Request")
                elif path == "/add-service-organization" and form.get("sender_scope", "parent") == "kid":
                    child_id = form.get("child_id", "")
                    result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': f'Error: {err}'})}")
                elif path in (
                    "/submit-instance",
                    "/submit-reading-log",
                    "/answer-reading-quiz",
                    "/redeem-reward",
                    "/submit-charity",
                    "/create-donation-pledge",
                    "/request-wallet-payout",
                    "/submit-service-hours",
                    "/child-login",
                    "/adopt-pet",
                    "/set-default-pet",
                    "/pet-care",
                    "/create-pet-species",
                    "/post-message",
                ):
                    child_id = form.get("child_id", "")
                    result = _redirect(f"/child?{urlencode({'child_id': child_id, 'msg': f'Error: {err}'})}")
                elif path in ("/parent-login", "/parent-request-reset", "/parent-request-reset-text", "/parent-reset", "/parent-logout"):
                    result = _redirect(f"/parent-login?{urlencode({'msg': f'Error: {err}'})}")
                else:
                    result = _redirect(f"/parent?{urlencode({'msg': f'Error: {err}'})}")
            else:
                result = _html_response(_layout("Error", f"<h2>Error</h2><p>{escape(str(err))}</p>"), status="500 Internal Server Error")

        status, headers, body = result
        start_response(status, headers)
        return [body]

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local family web interface")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8000, type=int)
    args = parser.parse_args()

    init_db()
    app = create_app()

    with make_server(args.host, args.port, app) as server:
        print(f"Serving Family UI on http://{args.host}:{args.port}")
        server.serve_forever()


if __name__ == "__main__":
    main()
