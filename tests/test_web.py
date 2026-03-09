from __future__ import annotations

from datetime import date, datetime
from io import BytesIO

from src.family_system import db, web
from src.family_system.repository import (
    apply_birthday_treatment,
    add_manual_ledger_entry,
    add_or_update_child,
    add_task,
    add_task_schedule,
    adopt_weekly_pet,
    award_weekly_pet_badges,
    complete_pet_care,
    complete_donation_pledge,
    create_donation_pledge,
    create_pet_species,
    current_week_key,
    delete_task_schedule,
    generate_task_instances,
    generate_pet_help_messages,
    get_current_pet,
    list_balances,
    pet_care_streak_weeks,
    list_due_task_instances,
    list_messages,
    list_charities,
    list_donation_pledges,
    list_child_house_rules,
    list_pet_species,
    list_service_organizations,
    list_service_entries,
    list_task_schedules,
    list_tasks,
    list_wallet_payouts,
    list_reading_logs,
    get_wallet_daily_interest_rate_percent,
    total_interest_earned,
    list_motd_library,
    list_fun_fact_library,
    get_child_activities_for_date,
    add_holiday,
    apply_daily_wallet_interest,
    create_child_pin_reset_token,
    consume_child_pin_reset_token,
    add_child_activity,
    add_weekly_allowance_plan_item,
    list_pending_activity_notifications,
    mark_activity_notification_sent,
    update_activity_notification_settings,
    mark_wallet_payout_sent,
    record_task_completion,
    request_wallet_payout,
    review_task_completion,
    review_service_hours,
    set_service_credit_rates,
    set_task_schedule_active,
    set_child_pin,
    set_after_school_reminders,
    set_weekly_allowance_default_amount,
    set_weekly_allowance_override_amount,
    get_message_of_the_day,
    get_fun_fact_of_the_day,
    get_screen_time_allotment_minutes,
    get_weekly_allowance_status,
    list_child_app_limits,
    add_concert_goal,
    add_adventure_goal,
    list_child_concert_goal_progress,
    list_child_adventure_goal_progress,
    list_ios_usage_reports,
    clone_weekly_allowance_override_from_default,
    update_child_contact_info,
    verify_child_pin,
    set_message_of_the_day,
    set_fun_fact_of_the_day,
    submit_service_hours,
    submit_task_instance,
    total_completed_service_hours,
    weekly_required_completed_count,
    list_weekly_allowance_plan_items,
    update_task_schedule,
)


def _invoke_app(
    path: str = "/",
    method: str = "GET",
    body: str = "",
    cookie: str = "",
    auth_header: str = "",
) -> tuple[str, bytes]:
    app = web.create_app()
    status_holder: list[str] = []
    if "?" in path:
        path_info, query_string = path.split("?", 1)
    else:
        path_info, query_string = path, ""

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        status_holder.append(status)

    payload = body.encode("utf-8")
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path_info,
        "QUERY_STRING": query_string,
        "CONTENT_LENGTH": str(len(payload)),
        "wsgi.input": BytesIO(payload),
    }
    if cookie:
        environ["HTTP_COOKIE"] = cookie
    if auth_header:
        environ["HTTP_AUTHORIZATION"] = auth_header
    chunks = app(environ, start_response)
    content = b"".join(chunks)
    return status_holder[0], content


def test_home_page_renders_children(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    add_or_update_child("Elliana", 10)
    add_or_update_child("Gracelyn", 7)
    add_task("Read", "required", "allowance", 1)

    status, content = _invoke_app("/")

    assert status.startswith("200")
    text = content.decode("utf-8")
    assert "Ploudeville Family Task &amp; Reward System" in text
    assert "Elliana" in text
    assert "Gracelyn" in text


def test_child_portal_requires_pin(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    set_child_pin(child_id, "1234")

    status, content = _invoke_app(f"/child?child_id={child_id}")

    assert status.startswith("200")
    assert "PIN Login" in content.decode("utf-8")


def test_recurring_schedule_generates_due_instances(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    task_id = add_task("Math practice", "required", "allowance", 1)
    add_task_schedule(task_id=task_id, cadence="daily", child_id=child_id, due_time="18:00")

    created = generate_task_instances("2026-02-14", "2026-02-16")
    due = list_due_task_instances(child_id=child_id, due_date="2026-02-15")

    assert created >= 3
    assert len(due) == 1
    assert due[0]["task_name"] == "Math practice"


def test_schedule_can_be_updated_and_disabled(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Rosie", 3)
    task_id = add_task("Clean up toys", "optional", "screen_time", 5)
    schedule_id = add_task_schedule(task_id=task_id, cadence="daily", child_id=child_id, due_time="17:30")

    updated = update_task_schedule(
        schedule_id=schedule_id,
        task_id=task_id,
        cadence="weekly",
        child_id=child_id,
        day_of_week=5,
        due_time="18:15",
    )
    assert updated

    toggled = set_task_schedule_active(schedule_id=schedule_id, active=False)
    assert toggled

    schedules = list_task_schedules(active_only=False)
    row = next(s for s in schedules if s["id"] == schedule_id)
    assert row["cadence"] == "weekly"
    assert row["day_of_week"] == 5
    assert row["due_time"] == "18:15"
    assert row["active"] == 0


def test_schedule_delete_safety_checks(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    task_id = add_task("Reading", "required", "allowance", 1)

    # Schedule with linked instances should not be deletable.
    protected_id = add_task_schedule(task_id=task_id, cadence="daily", child_id=child_id)
    generate_task_instances("2026-02-14", "2026-02-14")
    try:
        delete_task_schedule(protected_id)
        assert False, "Expected ValueError for linked instances"
    except ValueError as err:
        assert "linked task instances" in str(err)

    # Fresh schedule with no instances can be deleted.
    deletable_id = add_task_schedule(task_id=task_id, cadence="weekly", child_id=child_id, day_of_week=6)
    assert delete_task_schedule(deletable_id) is True


def test_parent_filter_hides_inactive_schedules(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Rosie", 3)
    task_id = add_task("Pick up blocks", "optional", "points", 2)
    schedule_id = add_task_schedule(task_id=task_id, cadence="daily", child_id=child_id)
    set_task_schedule_active(schedule_id, active=False)

    status, content = _invoke_app("/parent?show_inactive=0")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert f'name="schedule_id" value="{schedule_id}"' not in html


def test_default_weekly_allowance_plan_credits_once_after_all_approvals(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    dish_id = add_task("Unload dishwasher weekly", "required", "allowance", 3)
    trash_id = add_task("Take trash out weekly", "required", "allowance", 4)

    assert set_weekly_allowance_default_amount(child_id, 12.5)
    add_weekly_allowance_plan_item(child_id=child_id, task_id=dish_id, day_of_week=0, due_time="08:00")
    add_weekly_allowance_plan_item(child_id=child_id, task_id=trash_id, day_of_week=2, due_time="08:00")

    created = generate_task_instances("2026-02-16", "2026-02-22")
    due = list_due_task_instances(child_id=child_id)
    assert created >= 2
    assert len(due) == 2

    first_completion = submit_task_instance(int(due[0]["id"]), "done")
    assert review_task_completion(first_completion, "approved", "Mom")
    balances_after_first = list_balances()
    assert not any(
        int(row["child_id"]) == child_id and str(row["asset_type"]) == "allowance"
        for row in balances_after_first
    )

    second_completion = submit_task_instance(int(due[1]["id"]), "done")
    assert review_task_completion(second_completion, "approved", "Mom")
    balances_after_second = list_balances()
    second_allowance = next(float(row["balance"]) for row in balances_after_second if row["asset_type"] == "allowance")
    assert second_allowance == 12.5

    status = get_weekly_allowance_status(child_id, "2026-W08")
    assert status["approved_count"] == 2
    assert status["total_planned"] == 2
    assert status["credited"] is True
    assert status["allowance_amount"] == 12.5


def test_weekly_allowance_all_days_rule_generates_daily_instances(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    task_id = add_task("Read 20 minutes", "required", "allowance", 0)

    assert set_weekly_allowance_default_amount(child_id, 9.0)
    add_weekly_allowance_plan_item(
        child_id=child_id,
        task_id=task_id,
        period_mode="all_days",
        due_time="18:00",
    )

    created = generate_task_instances("2026-02-16", "2026-02-22")
    due = list_due_task_instances(child_id=child_id)
    status = get_weekly_allowance_status(child_id, "2026-W08")

    assert created >= 7
    assert len(due) == 7
    assert status["total_planned"] == 7


def test_weekly_allowance_times_in_period_rule_credits_after_target_count(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    task_id = add_task("Unload dishwasher", "required", "allowance", 0)

    assert set_weekly_allowance_default_amount(child_id, 6.0)
    add_weekly_allowance_plan_item(
        child_id=child_id,
        task_id=task_id,
        period_mode="times_per_period",
        times_per_period=2,
    )

    created = generate_task_instances("2026-02-16", "2026-02-22")
    due = list_due_task_instances(child_id=child_id)
    assert created >= 2
    assert len(due) == 2

    first_completion = submit_task_instance(int(due[0]["id"]), "done")
    assert review_task_completion(first_completion, "approved", "Mom")
    first_status = get_weekly_allowance_status(child_id, "2026-W08")
    assert first_status["approved_count"] == 1
    assert first_status["credited"] is False

    second_completion = submit_task_instance(int(due[1]["id"]), "done")
    assert review_task_completion(second_completion, "approved", "Mom")
    second_status = get_weekly_allowance_status(child_id, "2026-W08")
    assert second_status["approved_count"] == 2
    assert second_status["total_planned"] == 2
    assert second_status["credited"] is True


def test_current_week_override_shadows_default_weekly_allowance_plan(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    default_task_id = add_task("Default room reset", "required", "allowance", 2)
    override_task_id = add_task("Override laundry fold", "required", "allowance", 2)

    assert set_weekly_allowance_default_amount(child_id, 10.0)
    add_weekly_allowance_plan_item(child_id=child_id, task_id=default_task_id, day_of_week=0, due_time="09:00")
    add_weekly_allowance_plan_item(child_id=child_id, task_id=default_task_id, day_of_week=2, due_time="09:00")

    copied = clone_weekly_allowance_override_from_default(child_id, "2026-W08")
    assert copied == 2
    assert set_weekly_allowance_override_amount(child_id, "2026-W08", 7.0)
    add_weekly_allowance_plan_item(
        child_id=child_id,
        task_id=override_task_id,
        day_of_week=4,
        due_time="10:00",
        week_key="2026-W08",
    )

    plan_items = list_weekly_allowance_plan_items(child_id=child_id, include_inactive=False)
    override_items = [
        row for row in plan_items if str(row["plan_scope"]) == "weekly_allowance_override" and str(row["week_key"]) == "2026-W08"
    ]
    assert len(override_items) == 3

    created = generate_task_instances("2026-02-16", "2026-02-22")
    due = list_due_task_instances(child_id=child_id)
    task_names = {str(row["task_name"]) for row in due}
    assert created >= 3
    assert "Override laundry fold" in task_names
    assert len(due) == 3

    for item in due:
        completion_id = submit_task_instance(int(item["id"]), "done")
        assert review_task_completion(completion_id, "approved", "Dad")

    balances = list_balances()
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance == 7.0

    status = get_weekly_allowance_status(child_id, "2026-W08")
    assert status["plan_source"] == "override"
    assert status["allowance_amount"] == 7.0
    assert status["approved_count"] == 3
    assert status["credited"] is True


def test_parent_page_shows_weekly_allowance_section(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Rosie", 3)
    task_id = add_task("Toy pickup weekly", "required", "allowance", 1)
    set_weekly_allowance_default_amount(child_id, 5.0)
    add_weekly_allowance_plan_item(child_id=child_id, task_id=task_id, day_of_week=5, due_time="12:00")

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, content = _invoke_app("/parent", cookie=parent_cookie)

    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Weekly Allowance Plans" in html
    assert "Default Week" in html
    assert "Current Week Override" in html
    assert "Saved Default Rules" in html
    assert "Saved Override Rules" in html
    assert "Toy pickup weekly" in html
    assert "Default Period" in html
    assert "Plan Details By Child" in html
    assert f'href="#weekly-plan-child-{child_id}"' in html
    assert "All Days" in html
    assert "Any Day In Period" in html
    assert "Times In Period" in html


def test_parent_weekly_allowance_details_show_default_rules_and_current_deliverables(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    read_task_id = add_task("Read 20 minutes", "required", "allowance", 0)
    dish_task_id = add_task("Unload dishwasher", "required", "allowance", 0)
    set_weekly_allowance_default_amount(child_id, 10.0)
    add_weekly_allowance_plan_item(child_id=child_id, task_id=read_task_id, period_mode="all_days", due_time="18:00")
    add_weekly_allowance_plan_item(child_id=child_id, task_id=dish_task_id, period_mode="times_per_period", times_per_period=2)

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, content = _invoke_app("/parent", cookie=parent_cookie)

    assert status.startswith("200")
    html = content.decode("utf-8")
    assert f'id="weekly-plan-child-{child_id}"' in html
    assert "Default Period Rules" in html
    assert "Current Period Deliverables" in html
    assert "Monday: Read 20 minutes" in html
    assert "Any Day In Period #1: Unload dishwasher" in html
    assert "Any Day In Period #2: Unload dishwasher" in html


def test_pet_species_contains_unicorn_and_alacorn(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    names = {row["name"] for row in list_pet_species()}
    assert "Unicorn" in names
    assert "Alacorn" in names


def test_pet_unlock_adoption_and_help_messages(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    task_id = add_task("Reading", "required", "allowance", 1)
    for _ in range(3):
        completion_id = record_task_completion(child_id=child_id, task_id=task_id, completion_note="done")
        review_task_completion(completion_id, "approved", "Mom")

    assert weekly_required_completed_count(child_id, current_week_key()) >= 3
    unicorn = next(p for p in list_pet_species() if p["name"] == "Unicorn")
    adopt_weekly_pet(child_id=child_id, pet_species_id=int(unicorn["id"]), pet_name="Sparkles")

    pet = get_current_pet(child_id)
    assert pet is not None
    assert pet["pet_name"] == "Sparkles"

    complete_pet_care(child_id, "feed")
    complete_pet_care(child_id, "water")
    complete_pet_care(child_id, "nurture")
    awarded = award_weekly_pet_badges(child_id)
    assert awarded >= 1
    assert pet_care_streak_weeks(child_id) >= 1

    # Create a custom species as a child.
    create_pet_species("Biscuit Dragon", rarity="mythic", created_by_child_id=child_id)
    species_names = {row["name"] for row in list_pet_species()}
    assert "Biscuit Dragon" in species_names

    # Leave care missing in another week to trigger pet help messages.
    generate_pet_help_messages(child_id, week_key="2099-W01")
    msgs = list_messages(child_id=child_id)
    text_blob = " ".join(m["message_text"] for m in msgs)
    assert "need food" in text_blob
    assert "need water" in text_blob
    assert "need love and nurture" in text_blob


def test_parent_page_has_pet_center_section(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)

    status, content = _invoke_app("/parent", cookie=f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Pet Center" in html
    assert "Available Pets" in html


def test_parent_page_requires_auth(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)

    status, content = _invoke_app("/parent")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Parent Panel Sign In" in html
    assert "Text Reset Link" in html


def test_parent_reset_page_renders_from_token(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    status, content = _invoke_app("/parent-reset?token=test-token")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Reset Parent Password" in html
    assert 'name="token" value="test-token"' in html


def test_birthday_treatment_awards_bonus_and_message(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10, "2015-11-22")

    applied = apply_birthday_treatment(child_id, date(2026, 11, 22))
    assert applied is True

    balances = list_balances(child_id=child_id)
    by_asset = {row["asset_type"]: float(row["balance"]) for row in balances}
    assert by_asset["allowance"] >= 11.0
    assert by_asset["screen_time"] >= 110.0

    msgs = list_messages(child_id=child_id)
    text = " ".join(m["message_text"] for m in msgs)
    assert "Happy Birthday Elliana" in text


def test_seed_charities_available(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    names = {row["name"] for row in list_charities()}
    assert "The Nature Conservancy" in names
    assert "charity: water" in names
    assert "National Alliance to End Homelessness" in names
    assert "St. Jude Children's Research Hospital" in names
    assert "Direct Relief" in names


def test_donation_completion_deducts_allowance(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 20.0, "Seed allowance")
    charity = next(ch for ch in list_charities() if ch["name"] == "The Nature Conservancy")
    pledge_id = create_donation_pledge(child_id=child_id, charity_id=int(charity["id"]), amount=10.0, note="Save whales")
    assert complete_donation_pledge(pledge_id=pledge_id, completed_by="Mom")

    balances = list_balances(child_id=child_id)
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance == 10.0

    pledges = list_donation_pledges(child_id=child_id)
    pledge = next(p for p in pledges if int(p["id"]) == pledge_id)
    assert pledge["status"] == "completed"


def test_wallet_payout_queue_and_send(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Gracelyn", 7)
    add_manual_ledger_entry(child_id, "allowance", 25.0, "Seed allowance")

    payout_id = request_wallet_payout(child_id=child_id, amount=10.0, note="Weekly allowance")
    queued = list_wallet_payouts(child_id=child_id)
    payout = next(p for p in queued if int(p["id"]) == payout_id)
    assert payout["status"] == "pending_parent"

    assert mark_wallet_payout_sent(payout_id=payout_id, sent_by="Mom", transfer_reference="apple-cash-123")

    balances = list_balances(child_id=child_id)
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance == 15.0

    updated = list_wallet_payouts(child_id=child_id)
    payout_done = next(p for p in updated if int(p["id"]) == payout_id)
    assert payout_done["status"] == "sent"
    assert payout_done["transfer_reference"] == "apple-cash-123"


def test_home_page_shows_wallet_and_donated_totals(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 20.0, "seed")
    charity = next(ch for ch in list_charities() if ch["name"] == "The Nature Conservancy")
    pledge_id = create_donation_pledge(child_id=child_id, charity_id=int(charity["id"]), amount=10.0, note="save whales")
    assert complete_donation_pledge(pledge_id=pledge_id, completed_by="Mom")

    status, content = _invoke_app("/")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Wallet $10.01" in html
    assert "Donated to charities: $10.00" in html


def test_daily_wallet_interest_applies_once_per_day_and_shows_on_home(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 100.0, "seed")

    status_first, content_first = _invoke_app("/")
    assert status_first.startswith("200")
    html_first = content_first.decode("utf-8")
    assert "Daily wallet interest rate" in html_first
    assert "Interest earned: $0.05" in html_first

    balances = list_balances(child_id=child_id)
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance == 100.05
    assert total_interest_earned(child_id) == 0.05

    status_second, _ = _invoke_app("/")
    assert status_second.startswith("200")
    balances_after = list_balances(child_id=child_id)
    allowance_after = next(float(row["balance"]) for row in balances_after if row["asset_type"] == "allowance")
    assert allowance_after == 100.05
    assert total_interest_earned(child_id) == 0.05


def test_parent_can_update_daily_interest_rate(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)

    cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, _ = _invoke_app(
        "/update-interest-rate",
        method="POST",
        body="rate_percent=0.1",
        cookie=cookie,
    )
    assert status.startswith("303")
    assert get_wallet_daily_interest_rate_percent() == 0.1


def test_service_hours_completed_adds_configured_credits(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Rosie", 3)
    set_service_credit_rates(allowance_per_hour=2.0, screen_minutes_per_hour=15.0)
    org_id = int(next(o for o in list_service_organizations() if "Surfrider Foundation" in str(o["name"]))["id"])

    entry_id = submit_service_hours(
        child_id=child_id,
        organization_id=org_id,
        hours=2.0,
        service_date="2026-02-14",
        note="Beach cleanup",
    )
    assert review_service_hours(entry_id=entry_id, decision="completed", reviewed_by="Mom")

    balances = list_balances(child_id=child_id)
    by_asset = {str(row["asset_type"]): float(row["balance"]) for row in balances}
    assert by_asset["allowance"] == 4.0
    assert by_asset["screen_time"] == 30.0

    entries = list_service_entries(child_id=child_id)
    row = next(r for r in entries if int(r["id"]) == entry_id)
    assert row["status"] == "completed"
    assert total_completed_service_hours(child_id=child_id) == 2.0


def test_home_page_shows_service_hours_total(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    org_id = int(next(o for o in list_service_organizations() if "Habitat for Humanity" in str(o["name"]))["id"])
    entry_id = submit_service_hours(
        child_id=child_id,
        organization_id=org_id,
        hours=1.5,
        service_date="2026-02-14",
        note="Build day",
    )
    assert review_service_hours(entry_id=entry_id, decision="completed", reviewed_by="Dad")

    status, content = _invoke_app("/")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Lifetime service hours: 1.50" in html


def test_init_db_deactivates_old_placeholder_service_orgs(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    # Simulate an old placeholder org existing in an upgraded DB.
    from src.family_system.db import get_connection
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO service_organizations (name, website, active)
            VALUES (?, ?, 1)
            """,
            ("The Nature Conservancy", "https://www.nature.org"),
        )

    # Re-run init to apply migration cleanup.
    db.init_db()

    active_names = {str(o["name"]) for o in list_service_organizations(active_only=True)}
    assert "The Nature Conservancy" not in active_names


def test_home_page_shows_child_house_rules_and_reminders(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)
    add_or_update_child("Rosie", 3)
    set_after_school_reminders("homework,practice voice,practice piano,exercise,read")

    status, content = _invoke_app("/")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "After School Reminders" in html
    assert "homework, practice voice, practice piano, exercise, read" in html
    assert "Sun-Thu: screen off 20:15, bedtime 21:00" in html
    assert "Sun-Thu: screen off 19:45, bedtime 20:00" in html


def test_parent_can_update_child_house_rules_route(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Gracelyn", 7)

    body = (
        f"child_id={child_id}&weekday_screen_off=20%3A00&weekday_bedtime=20%3A30"
        "&weekend_screen_off=21%3A15&weekend_bedtime=21%3A45"
    )
    status, _ = _invoke_app(
        "/update-child-house-rules",
        method="POST",
        body=body,
        cookie=f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}",
    )
    assert status.startswith("303")

    rows = list_child_house_rules(active_only=True)
    row = next(r for r in rows if int(r["child_id"]) == child_id)
    assert row["weekday_screen_off"] == "20:00"
    assert row["weekday_bedtime"] == "20:30"


def test_motd_and_fun_fact_seed_sizes(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    assert len(list_motd_library(active_only=True, limit=500)) >= 200
    assert len(list_fun_fact_library(active_only=True, limit=500)) >= 365


def test_home_page_shows_motd_and_fun_fact(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)
    today = date.today().isoformat()
    set_message_of_the_day(today, set_by="Mom", message_text="You are brave and kind.")
    set_fun_fact_of_the_day(today, set_by="Mom", fact_text="Wild fact: Honey never spoils.")

    status, content = _invoke_app("/")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Message of the Day" in html
    assert "You are brave and kind." in html
    assert "Fun Fact of the Day" in html
    assert "Honey never spoils" in html


def test_motd_and_fun_fact_have_default_daily_assignment(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    motd = get_message_of_the_day("2026-02-14")
    fun_fact = get_fun_fact_of_the_day("2026-02-14")
    assert motd is not None
    assert fun_fact is not None
    assert motd["set_by"] == "system_default"
    assert fun_fact["set_by"] == "system_default"


def test_motd_default_rotation_has_clear_daily_variation(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    m1 = get_message_of_the_day("2026-02-15")
    m2 = get_message_of_the_day("2026-02-16")
    assert m1 is not None and m2 is not None
    assert m1["message_text"] != m2["message_text"]


def test_default_child_activities_seed_and_holiday_no_school(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    ellie_id = add_or_update_child("Elliana", 10)
    rosie_id = add_or_update_child("Rosie", 3)
    db.init_db()  # Apply default activity seeds after children exist.

    # Monday activities: school for Ellie, school for Rosie.
    ellie_mon = get_child_activities_for_date(ellie_id, "2026-02-16")
    rosie_sat = get_child_activities_for_date(rosie_id, "2026-02-14")
    assert any(a["activity_name"] == "School" for a in ellie_mon)
    assert any(a["activity_name"] == "Soccer" for a in rosie_sat)

    # Holiday removes school entries for that date.
    add_holiday("2026-02-16", "Presidents Day", no_school=True)
    ellie_holiday = get_child_activities_for_date(ellie_id, "2026-02-16")
    assert any(a["category"] == "holiday" for a in ellie_holiday)
    assert not any(a["category"] == "school" for a in ellie_holiday)


def test_child_contact_update_and_pin_reset_token_flow(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    assert update_child_contact_info(child_id, "eploude@icloud.com", "555-111-2222")

    token, email, child_name = create_child_pin_reset_token(child_id, valid_minutes=30)
    assert email == "eploude@icloud.com"
    assert child_name == "Elliana"

    assert consume_child_pin_reset_token(token, "2468")
    assert verify_child_pin(child_id, "2468")


def test_activity_notification_settings_and_pending_detection(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Rosie", 3, email="rploude@icloud.com", text_number="5551234567")
    activity_id = add_child_activity(
        child_id=child_id,
        activity_name="Soccer",
        category="sports",
        day_of_week=5,
        start_time="09:45",
        notify_enabled=True,
        notify_minutes_before=30,
        notify_channels="both",
    )
    assert update_activity_notification_settings(
        activity_id=activity_id,
        notify_enabled=True,
        notify_minutes_before=15,
        notify_channels="both",
    )

    pending = list_pending_activity_notifications(datetime.strptime("2026-02-14 09:35", "%Y-%m-%d %H:%M"))
    channels = {p["channel"] for p in pending if int(p["activity_id"]) == activity_id}
    assert channels == {"email", "sms"}

    assert mark_activity_notification_sent(activity_id, "2026-02-14", "email", "rploude@icloud.com")
    pending_after = list_pending_activity_notifications(datetime.strptime("2026-02-14 09:40", "%Y-%m-%d %H:%M"))
    channels_after = {p["channel"] for p in pending_after if int(p["activity_id"]) == activity_id}
    assert channels_after == {"sms"}


def test_default_screen_time_allotment_weekday_weekend(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    assert get_screen_time_allotment_minutes("2026-02-13") == 60  # Friday
    assert get_screen_time_allotment_minutes("2026-02-14") == 120  # Saturday


def test_default_roblox_limits_per_child(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    ellie_id = add_or_update_child("Elliana", 10)
    gracie_id = add_or_update_child("Gracelyn", 7)
    rosie_id = add_or_update_child("Rosie", 3)

    by_child = {
        int(row["child_id"]): int(row["minutes_per_day"])
        for row in list_child_app_limits(child_id=None, active_only=True)
        if str(row["app_name"]) == "Roblox"
    }
    assert by_child[ellie_id] == 60
    assert by_child[gracie_id] == 60
    assert by_child[rosie_id] == 0


def test_child_portal_shows_app_time_limits(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    set_child_pin(child_id, "1234")

    login_status, _ = _invoke_app("/child-login", method="POST", body=f"child_id={child_id}&pin=1234")
    assert login_status.startswith("303")
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"
    status, content = _invoke_app(f"/child?child_id={child_id}", cookie=cookie)
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "App Time Limits" in html
    assert "Roblox" in html


def test_concert_goal_progress_uses_80_20_split(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 80.0, "seed allowance")
    add_concert_goal(
        child_id=child_id,
        artist_name="Sabrina Carpenter",
        event_name="Sabrina Carpenter Live",
        event_date="2026-08-01",
        low_price=100.0,
        high_price=250.0,
    )
    rows = list_child_concert_goal_progress(child_id)
    assert rows
    row = rows[0]
    assert float(row["kid_target_amount"]) == 80.0
    assert float(row["parent_share_amount"]) == 20.0
    assert bool(row["eligible"]) is True


def test_child_portal_shows_concert_rewards_section(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Gracelyn", 7)
    set_child_pin(child_id, "1234")
    add_concert_goal(
        child_id=child_id,
        artist_name="Benson Boone",
        event_name="Benson Boone Tour",
        event_date="2026-09-10",
        low_price=120.0,
        high_price=300.0,
    )
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"
    status, content = _invoke_app(f"/child?child_id={child_id}", cookie=cookie)
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Concert Rewards (80/20)" in html
    assert "Benson Boone" in html


def test_adventure_goal_progress_uses_80_20_split(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 100.0, "seed allowance")
    add_adventure_goal(
        child_id=child_id,
        park_name="Disneyland Park",
        ticket_name="Day Pass",
        target_date="2026-07-15",
        low_price=120.0,
        high_price=220.0,
        region="Southern California",
        category="theme_park",
    )
    rows = list_child_adventure_goal_progress(child_id)
    assert rows
    row = rows[0]
    assert float(row["kid_target_amount"]) == 96.0
    assert float(row["parent_share_amount"]) == 24.0
    assert bool(row["eligible"]) is True


def test_child_portal_shows_adventure_park_rewards_section(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Gracelyn", 7)
    set_child_pin(child_id, "1234")
    add_adventure_goal(
        child_id=child_id,
        park_name="Hersheypark",
        ticket_name="Summer Day Ticket",
        target_date="2026-08-20",
        low_price=80.0,
        high_price=140.0,
        region="Eastern Pennsylvania",
        category="theme_park",
    )
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"
    status, content = _invoke_app(f"/child?child_id={child_id}", cookie=cookie)
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Adventure Parks (80/20)" in html
    assert "Hersheypark" in html


def test_reading_log_quiz_pass_awards_task_credit(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    set_child_pin(child_id, "1234")
    add_task("Read", "required", "allowance", 1)
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"

    submit_body = (
        f"child_id={child_id}&read_date=2026-02-17&start_time=16%3A00&end_time=16%3A30"
        "&book_title=Charlotte%27s+Web&chapters=1-2"
    )
    status_submit, _ = _invoke_app("/submit-reading-log", method="POST", body=submit_body, cookie=cookie)
    assert status_submit.startswith("303")

    logs = list_reading_logs(child_id=child_id, limit=10)
    assert logs
    log_id = int(logs[0]["id"])
    assert logs[0]["status"] == "awaiting_answers"
    assert str(logs[0]["question_1"]).strip()
    assert str(logs[0]["question_2"]).strip()

    answer_body = (
        f"child_id={child_id}&log_id={log_id}"
        "&answer_1=Wilbur+felt+nervous+at+first+but+started+trusting+new+friends+as+the+story+developed."
        "&answer_2=One+important+lesson+is+that+friendship+and+kindness+can+change+someone%27s+future."
    )
    status_answer, _ = _invoke_app("/answer-reading-quiz", method="POST", body=answer_body, cookie=cookie)
    assert status_answer.startswith("303")

    updated_logs = list_reading_logs(child_id=child_id, limit=10)
    top = updated_logs[0]
    assert top["status"] == "passed"
    assert int(top["passed"]) == 1
    assert top["credit_completion_id"] is not None

    balances = list_balances(child_id=child_id)
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance >= 1.0


def test_reading_log_quiz_fail_does_not_award_credit(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Gracelyn", 7)
    set_child_pin(child_id, "1234")
    add_task("Reading", "required", "allowance", 1)
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"

    submit_body = (
        f"child_id={child_id}&read_date=2026-02-17&start_time=17%3A00&end_time=17%3A20"
        "&book_title=Magic+Tree+House&chapters=3-4"
    )
    _invoke_app("/submit-reading-log", method="POST", body=submit_body, cookie=cookie)
    logs = list_reading_logs(child_id=child_id, limit=10)
    log_id = int(logs[0]["id"])

    answer_body = (
        f"child_id={child_id}&log_id={log_id}"
        "&answer_1=It+was+good."
        "&answer_2=Be+nice."
    )
    status_answer, _ = _invoke_app("/answer-reading-quiz", method="POST", body=answer_body, cookie=cookie)
    assert status_answer.startswith("303")

    updated_logs = list_reading_logs(child_id=child_id, limit=10)
    top = updated_logs[0]
    assert top["status"] == "failed"
    assert int(top["passed"]) == 0
    assert top["credit_completion_id"] is None


def test_parent_page_shows_reading_review_table(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    set_child_pin(child_id, "1234")
    add_task("Read", "required", "allowance", 1)
    cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"

    submit_body = (
        f"child_id={child_id}&read_date=2026-02-17&start_time=16%3A00&end_time=16%3A30"
        "&book_title=Charlotte%27s+Web&chapters=1-2"
    )
    _invoke_app("/submit-reading-log", method="POST", body=submit_body, cookie=cookie)
    logs = list_reading_logs(child_id=child_id, limit=10)
    log_id = int(logs[0]["id"])
    answer_body = (
        f"child_id={child_id}&log_id={log_id}"
        "&answer_1=Wilbur+learned+that+he+could+be+brave+with+support+from+friends+in+hard+situations."
        "&answer_2=Kindness+and+loyalty+from+friends+can+change+someone%27s+future+for+the+better."
    )
    _invoke_app("/answer-reading-quiz", method="POST", body=answer_body, cookie=cookie)

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, content = _invoke_app("/parent", cookie=parent_cookie)
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Reading Review" in html
    assert "Charlotte&#x27;s Web" in html
    assert "Wilbur learned" in html


def test_parent_reading_review_filters_and_export_csv(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    ellie_id = add_or_update_child("Elliana", 10)
    gracie_id = add_or_update_child("Gracelyn", 7)
    set_child_pin(ellie_id, "1234")
    set_child_pin(gracie_id, "1234")
    add_task("Read", "required", "allowance", 1)

    ellie_cookie = f"{web.COOKIE_NAME}={ellie_id}:{web._sign_child(ellie_id)}"
    gracie_cookie = f"{web.COOKIE_NAME}={gracie_id}:{web._sign_child(gracie_id)}"

    _invoke_app(
        "/submit-reading-log",
        method="POST",
        body=(
            f"child_id={ellie_id}&read_date=2026-02-17&start_time=16%3A00&end_time=16%3A30"
            "&book_title=Ellie+Book&chapters=1-2"
        ),
        cookie=ellie_cookie,
    )
    ellie_log_id = int(list_reading_logs(child_id=ellie_id, limit=5)[0]["id"])
    _invoke_app(
        "/answer-reading-quiz",
        method="POST",
        body=(
            f"child_id={ellie_id}&log_id={ellie_log_id}"
            "&answer_1=Ellie+explained+the+main+events+with+detail+and+showed+clear+understanding+of+the+chapter."
            "&answer_2=Ellie+identified+the+theme+and+gave+examples+that+proved+she+understood+the+reading."
        ),
        cookie=ellie_cookie,
    )

    _invoke_app(
        "/submit-reading-log",
        method="POST",
        body=(
            f"child_id={gracie_id}&read_date=2026-02-16&start_time=17%3A00&end_time=17%3A20"
            "&book_title=Gracie+Book&chapters=3-4"
        ),
        cookie=gracie_cookie,
    )
    gracie_log_id = int(list_reading_logs(child_id=gracie_id, limit=5)[0]["id"])
    _invoke_app(
        "/answer-reading-quiz",
        method="POST",
        body=(
            f"child_id={gracie_id}&log_id={gracie_log_id}"
            "&answer_1=Good."
            "&answer_2=Nice."
        ),
        cookie=gracie_cookie,
    )

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, content = _invoke_app(
        f"/parent?reading_child_id={ellie_id}&reading_status=passed&reading_date_from=2026-02-17&reading_date_to=2026-02-17",
        cookie=parent_cookie,
    )
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Reading Review" in html
    assert "Ellie Book" in html
    assert "Gracie Book" not in html

    status_csv, content_csv = _invoke_app(
        f"/export-reading-csv?reading_child_id={ellie_id}&reading_status=passed&reading_date_from=2026-02-17&reading_date_to=2026-02-17",
        cookie=parent_cookie,
    )
    assert status_csv.startswith("200")
    csv_text = content_csv.decode("utf-8")
    assert "read_date,child_name,start_time,end_time,book_title,chapters" in csv_text
    assert "2026-02-17,Elliana" in csv_text
    assert "Gracie Book" not in csv_text


def test_parent_can_override_failed_reading_quiz_and_award_credit(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    set_child_pin(child_id, "1234")
    add_task("Read", "required", "allowance", 1)
    kid_cookie = f"{web.COOKIE_NAME}={child_id}:{web._sign_child(child_id)}"

    _invoke_app(
        "/submit-reading-log",
        method="POST",
        body=(
            f"child_id={child_id}&read_date=2026-02-17&start_time=16%3A00&end_time=16%3A30"
            "&book_title=Override+Book&chapters=1-2"
        ),
        cookie=kid_cookie,
    )
    log_id = int(list_reading_logs(child_id=child_id, limit=5)[0]["id"])
    _invoke_app(
        "/answer-reading-quiz",
        method="POST",
        body=f"child_id={child_id}&log_id={log_id}&answer_1=Short.&answer_2=Short.",
        cookie=kid_cookie,
    )
    failed_log = list_reading_logs(child_id=child_id, limit=5)[0]
    assert failed_log["status"] == "failed"
    assert failed_log["credit_completion_id"] is None

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, _ = _invoke_app(
        "/override-reading-credit",
        method="POST",
        body=f"log_id={log_id}&by=Mom",
        cookie=parent_cookie,
    )
    assert status.startswith("303")

    updated_log = list_reading_logs(child_id=child_id, limit=5)[0]
    assert updated_log["status"] == "passed"
    assert int(updated_log["passed"]) == 1
    assert updated_log["credit_completion_id"] is not None
    assert str(updated_log.get("parent_override_by") or "") == "Mom"
    balances = list_balances(child_id=child_id)
    allowance = next(float(row["balance"]) for row in balances if row["asset_type"] == "allowance")
    assert allowance >= 1.0

    status_parent, content_parent = _invoke_app("/parent", cookie=parent_cookie)
    assert status_parent.startswith("200")
    parent_html = content_parent.decode("utf-8")
    assert "Overridden: 1" in parent_html


def test_parent_page_shows_tab_navigation(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)
    status, content = _invoke_app("/parent", cookie=f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Daily Ops" in html
    assert "Allowance &amp; Money" in html
    assert "parent-tabbar" in html


def test_parent_can_add_task_without_per_task_value(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    parent_cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, _ = _invoke_app(
        "/add-task",
        method="POST",
        body="name=Unload+Dishwasher&rank=required&payout=allowance&value=",
        cookie=parent_cookie,
    )
    assert status.startswith("303")

    tasks = list_tasks()
    task = next(t for t in tasks if str(t["name"]) == "Unload Dishwasher")
    assert str(task["rank"]) == "required"
    assert str(task["payout_type"]) == "allowance"
    assert float(task["payout_value"]) == 0.0


def test_ios_usage_sync_endpoint_persists_report(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    payload = (
        '{'
        '"childName":"Elliana",'
        '"date":"2026-02-17",'
        '"totalMinutes":95,'
        '"perAppMinutes":{"Roblox":60,"YouTube Kids":20,"Safari":15}'
        '}'
    )
    status, content = _invoke_app("/api/v1/ios/usage-sync", method="POST", body=payload)
    assert status.startswith("200")
    assert '"ok": true' in content.decode("utf-8").lower()

    rows = list_ios_usage_reports(limit=10)
    assert rows
    assert rows[0]["child_name"] == "Elliana"


def test_ios_usage_sync_requires_bearer_when_configured(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    monkeypatch.setenv("IOS_SYNC_TOKEN", "secret-ios-token")
    payload = (
        '{'
        '"childName":"Elliana",'
        '"date":"2026-02-17",'
        '"totalMinutes":10,'
        '"perAppMinutes":{"Roblox":10}'
        '}'
    )
    status, content = _invoke_app("/api/v1/ios/usage-sync", method="POST", body=payload)
    assert status.startswith("401")
    assert "Unauthorized" in content.decode("utf-8")


def test_ios_safety_alert_endpoint_creates_message_and_notifies_parents(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    add_or_update_child("Elliana", 10)

    from src.family_system.db import get_connection
    with get_connection() as conn:
        conn.execute(
            "UPDATE parents SET text_number = ? WHERE active = 1",
            ("5551112222",),
        )

    email_calls: list[str] = []
    sms_calls: list[str] = []

    def fake_email(to_email: str, subject: str, content: str) -> None:
        email_calls.append(f"{to_email}|{subject}|{content}")

    def fake_sms(to_number: str, message_text: str) -> None:
        sms_calls.append(f"{to_number}|{message_text}")

    monkeypatch.setattr(web, "_send_plain_email", fake_email)
    monkeypatch.setattr(web, "_send_apeiron_sms", fake_sms)

    payload = (
        '{'
        '"childName":"Elliana",'
        '"date":"2026-02-17",'
        '"severity":"critical",'
        '"reason":"Concern reported (Bullying)",'
        '"notifyParentsEmail":true,'
        '"notifyParentsSms":true'
        '}'
    )
    status, content = _invoke_app("/api/v1/ios/safety-alert", method="POST", body=payload)
    assert status.startswith("200")
    body = content.decode("utf-8")
    assert '"ok": true' in body.lower()
    assert '"emailsent": 2' in body.lower()
    assert '"smssent": 2' in body.lower()

    msgs = list_messages()
    assert any("Concern reported (Bullying)" in str(m["message_text"]) for m in msgs)
    assert len(email_calls) == 2
    assert len(sms_calls) == 2


def test_ios_safety_alert_requires_bearer_when_configured(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    monkeypatch.setenv("IOS_SYNC_TOKEN", "secret-ios-token")
    payload = (
        '{'
        '"childName":"Elliana",'
        '"date":"2026-02-17",'
        '"severity":"warning",'
        '"reason":"High daily usage",'
        '"notifyParentsEmail":false,'
        '"notifyParentsSms":false'
        '}'
    )
    status, content = _invoke_app("/api/v1/ios/safety-alert", method="POST", body=payload)
    assert status.startswith("401")
    assert "Unauthorized" in content.decode("utf-8")

    status_ok, content_ok = _invoke_app(
        "/api/v1/ios/safety-alert",
        method="POST",
        body=payload,
        auth_header="Bearer secret-ios-token",
    )
    assert status_ok.startswith("200")
    assert '"ok": true' in content_ok.decode("utf-8").lower()


def test_parent_page_shows_ios_usage_reports_table(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    payload = (
        '{'
        '"childName":"Gracelyn",'
        '"date":"2026-02-17",'
        '"totalMinutes":42,'
        '"perAppMinutes":{"Roblox":30,"Safari":12}'
        '}'
    )
    _invoke_app("/api/v1/ios/usage-sync", method="POST", body=payload)
    status, content = _invoke_app("/parent", cookie=f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "iOS Usage Reports" in html
    assert "Gracelyn" in html


def test_parent_page_shows_interest_history_table(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    child_id = add_or_update_child("Elliana", 10)
    add_manual_ledger_entry(child_id, "allowance", 100.0, "seed")
    _invoke_app("/")  # trigger daily interest accrual

    status, content = _invoke_app("/parent", cookie=f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}")
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Interest History" in html
    assert "Opening Balance" in html
    assert "Elliana" in html


def test_parent_interest_history_filters_and_export_csv(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()

    ellie_id = add_or_update_child("Elliana", 10)
    gracie_id = add_or_update_child("Gracelyn", 7)
    add_manual_ledger_entry(ellie_id, "allowance", 100.0, "seed")
    add_manual_ledger_entry(gracie_id, "allowance", 50.0, "seed")
    apply_daily_wallet_interest("2026-02-14")
    apply_daily_wallet_interest("2026-02-15")

    cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    status, content = _invoke_app(
        f"/parent?interest_child_id={ellie_id}&interest_date_from=2026-02-15&interest_date_to=2026-02-15",
        cookie=cookie,
    )
    assert status.startswith("200")
    html = content.decode("utf-8")
    assert "Interest History" in html
    assert "2026-02-15" in html
    assert "2026-02-14" not in html

    status_csv, content_csv = _invoke_app(
        f"/export-interest-csv?interest_child_id={ellie_id}&interest_date_from=2026-02-15&interest_date_to=2026-02-15",
        cookie=cookie,
    )
    assert status_csv.startswith("200")
    csv_text = content_csv.decode("utf-8")
    assert "accrual_date,child_name,opening_balance,rate_percent,interest_amount" in csv_text
    assert "2026-02-15,Elliana" in csv_text
    assert "2026-02-14,Elliana" not in csv_text


def test_parent_export_routes_and_concert_goal_status_update(tmp_path) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    child_id = add_or_update_child("Elliana", 10)
    add_concert_goal(
        child_id=child_id,
        artist_name="Taylor Swift",
        event_name="Eras",
        event_date="2026-10-10",
        low_price=200.0,
        high_price=500.0,
    )
    goal_id = int(list_child_concert_goal_progress(child_id)[0]["id"])
    cookie = f"{web.PARENT_COOKIE_NAME}={web._sign_parent()}"
    # Set concert status.
    status_set, _ = _invoke_app(
        "/set-concert-goal-status",
        method="POST",
        body=f"goal_id={goal_id}&goal_status=purchased",
        cookie=cookie,
    )
    assert status_set.startswith("303")
    # Export endpoints.
    status_json, content_json = _invoke_app("/export-data-json", cookie=cookie)
    assert status_json.startswith("200")
    assert "concert_goals" in content_json.decode("utf-8")
    status_csv, content_csv = _invoke_app("/export-ledger-csv", cookie=cookie)
    assert status_csv.startswith("200")
    assert "child_name" in content_csv.decode("utf-8")


def test_reading_chatbot_adapter_requires_bearer_when_configured(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    monkeypatch.setenv("READING_CHATBOT_API_KEY", "adapter-key")

    status, content = _invoke_app(
        "/api/v1/reading-chatbot",
        method="POST",
        body="{\"action\":\"generate_questions\",\"book_title\":\"Charlotte's Web\",\"chapters\":\"1-2\",\"question_count\":2}",
    )
    assert status.startswith("401")
    assert "Unauthorized" in content.decode("utf-8")


def test_reading_chatbot_adapter_generate_and_evaluate(tmp_path, monkeypatch) -> None:
    db.DATA_DIR = tmp_path
    db.DB_PATH = tmp_path / "test_family.db"
    db.init_db()
    monkeypatch.setenv("READING_CHATBOT_API_KEY", "adapter-key")

    calls: list[str] = []

    def fake_groq_chat_json(system_prompt: str, user_prompt: str) -> dict:
        if "Generate exactly" in user_prompt:
            calls.append("generate")
            return {"questions": ["What happened first in the chapter?", "What lesson did you notice?"]}
        calls.append("evaluate")
        return {"passed": True, "score": 0.92}

    monkeypatch.setattr(web, "_groq_chat_json", fake_groq_chat_json)

    status_q, content_q = _invoke_app(
        "/api/v1/reading-chatbot",
        method="POST",
        body="{\"action\":\"generate_questions\",\"book_title\":\"Charlotte's Web\",\"chapters\":\"1-2\",\"question_count\":2}",
        auth_header="Bearer adapter-key",
    )
    assert status_q.startswith("200")
    text_q = content_q.decode("utf-8")
    assert "questions" in text_q
    assert "What happened first in the chapter?" in text_q

    status_e, content_e = _invoke_app(
        "/api/v1/reading-chatbot",
        method="POST",
        body=(
            '{"action":"evaluate_answers","book_title":"Charlotte\'s Web","chapters":"1-2",'
            '"questions":["Q1","Q2"],'
            '"answers":["Detailed answer one with enough context","Detailed answer two with enough context"]}'
        ),
        auth_header="Bearer adapter-key",
    )
    assert status_e.startswith("200")
    text_e = content_e.decode("utf-8")
    assert '"passed": true' in text_e.lower()
    assert '"score": 0.92' in text_e
    assert calls == ["generate", "evaluate"]
