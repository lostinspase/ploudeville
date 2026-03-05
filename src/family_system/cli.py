import argparse
from datetime import date, timedelta
from pprint import pprint

from .db import init_db
from .models import CompletionStatus, PayoutType, RewardType, ScheduleCadence, TaskRank
from .repository import (
    add_manual_ledger_entry,
    add_child,
    add_or_update_child,
    add_reward,
    add_task_schedule,
    add_task,
    generate_task_instances,
    list_children,
    list_balances,
    list_due_task_instances,
    list_task_schedules,
    list_task_completions,
    list_ledger_entries,
    list_rewards,
    list_tasks,
    redeem_reward,
    record_task_completion,
    set_child_pin,
    submit_task_instance,
    review_task_completion,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Family Task and Reward System CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialize database schema")

    add_child_parser = sub.add_parser("add-child", help="Add child")
    add_child_parser.add_argument("--name", required=True)
    add_child_parser.add_argument("--age", required=True, type=int)
    add_child_parser.add_argument("--birthdate", help="YYYY-MM-DD")

    sub.add_parser("list-children", help="List active children")
    sub.add_parser("seed-family", help="Seed/update default family children")

    add_task_parser = sub.add_parser("add-task", help="Add task")
    add_task_parser.add_argument("--name", required=True)
    add_task_parser.add_argument("--rank", required=True, choices=[r.value for r in TaskRank])
    add_task_parser.add_argument("--payout", required=True, choices=[p.value for p in PayoutType])
    add_task_parser.add_argument("--value", required=True, type=float)

    sub.add_parser("list-tasks", help="List active tasks")
    add_schedule_parser = sub.add_parser("add-schedule", help="Add recurring task schedule")
    add_schedule_parser.add_argument("--task-id", required=True, type=int)
    add_schedule_parser.add_argument("--cadence", required=True, choices=[c.value for c in ScheduleCadence])
    add_schedule_parser.add_argument("--child-id", type=int)
    add_schedule_parser.add_argument("--day-of-week", type=int, choices=range(0, 7))
    add_schedule_parser.add_argument("--due-time", help="Optional HH:MM")

    sub.add_parser("list-schedules", help="List task schedules")
    generate_instances_parser = sub.add_parser("generate-instances", help="Generate due task instances")
    generate_instances_parser.add_argument("--days", type=int, default=14)

    list_due_parser = sub.add_parser("list-due-tasks", help="List due task instances")
    list_due_parser.add_argument("--child-id", type=int)
    list_due_parser.add_argument("--date", help="YYYY-MM-DD (default: today)")

    add_reward_parser = sub.add_parser("add-reward", help="Add reward")
    add_reward_parser.add_argument("--name", required=True)
    add_reward_parser.add_argument("--type", required=True, choices=[r.value for r in RewardType])
    add_reward_parser.add_argument("--cost", required=True, type=float)

    sub.add_parser("list-rewards", help="List active rewards")
    list_balances_parser = sub.add_parser("list-balances", help="List wallet balances by child")
    list_balances_parser.add_argument("--child-id", type=int)
    list_ledger_parser = sub.add_parser("list-ledger", help="List ledger entries")
    list_ledger_parser.add_argument("--child-id", type=int)

    adjust_parser = sub.add_parser("adjust-balance", help="Manual ledger adjustment")
    adjust_parser.add_argument("--child-id", required=True, type=int)
    adjust_parser.add_argument("--asset", required=True, choices=[p.value for p in PayoutType])
    adjust_parser.add_argument("--amount", required=True, type=float)
    adjust_parser.add_argument("--note", default="")

    redeem_parser = sub.add_parser("redeem-reward", help="Redeem reward if enough balance")
    redeem_parser.add_argument("--child-id", required=True, type=int)
    redeem_parser.add_argument("--reward-id", required=True, type=int)
    redeem_parser.add_argument("--note", default="")

    complete_task_parser = sub.add_parser("complete-task", help="Record task completion by child")
    complete_task_parser.add_argument("--child-id", type=int)
    complete_task_parser.add_argument("--task-id", type=int)
    complete_task_parser.add_argument("--instance-id", type=int)
    complete_task_parser.add_argument("--note", default="")

    set_pin_parser = sub.add_parser("set-child-pin", help="Set a child PIN (4-8 digits)")
    set_pin_parser.add_argument("--child-id", required=True, type=int)
    set_pin_parser.add_argument("--pin", required=True)

    list_completions_parser = sub.add_parser("list-completions", help="List task completion records")
    list_completions_parser.add_argument(
        "--status",
        choices=[s.value for s in CompletionStatus],
    )
    list_completions_parser.add_argument("--child-id", type=int)

    review_parser = sub.add_parser("review-completion", help="Approve or reject a pending completion")
    review_parser.add_argument("--id", required=True, type=int)
    review_parser.add_argument(
        "--decision",
        required=True,
        choices=[CompletionStatus.APPROVED.value, CompletionStatus.REJECTED.value],
    )
    review_parser.add_argument("--by", required=True, help="Parent/guardian reviewer name")
    review_parser.add_argument("--note", default="")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
        print("Database initialized.")
        return

    if args.command == "add-child":
        child_id = add_child(args.name, args.age, args.birthdate)
        print(f"Child added with id={child_id}")
        return

    if args.command == "list-children":
        pprint(list_children())
        return

    if args.command == "seed-family":
        family = [
            ("Elliana", 10, "2015-11-22"),
            ("Gracelyn", 7, "2018-08-24"),
            ("Rosie", 3, "2022-09-07"),
        ]
        seeded = [
            {"id": add_or_update_child(name, age, birthdate), "name": name, "age": age, "birthdate": birthdate}
            for name, age, birthdate in family
        ]
        pprint(seeded)
        return

    if args.command == "add-task":
        task_id = add_task(args.name, args.rank, args.payout, args.value)
        print(f"Task added with id={task_id}")
        return

    if args.command == "list-tasks":
        pprint(list_tasks())
        return

    if args.command == "add-schedule":
        try:
            schedule_id = add_task_schedule(
                task_id=args.task_id,
                cadence=args.cadence,
                child_id=args.child_id,
                day_of_week=args.day_of_week,
                due_time=args.due_time,
            )
        except ValueError as err:
            parser.error(str(err))
        print(f"Schedule added with id={schedule_id}")
        return

    if args.command == "list-schedules":
        pprint(list_task_schedules())
        return

    if args.command == "generate-instances":
        start = date.today()
        end = start + timedelta(days=max(args.days - 1, 0))
        created = generate_task_instances(start, end)
        print(f"Generated {created} task instances from {start.isoformat()} to {end.isoformat()}")
        return

    if args.command == "list-due-tasks":
        target_date = args.date or date.today().isoformat()
        pprint(list_due_task_instances(child_id=args.child_id, due_date=target_date))
        return

    if args.command == "add-reward":
        reward_id = add_reward(args.name, args.type, args.cost)
        print(f"Reward added with id={reward_id}")
        return

    if args.command == "list-rewards":
        pprint(list_rewards())
        return

    if args.command == "list-balances":
        pprint(list_balances(child_id=args.child_id))
        return

    if args.command == "list-ledger":
        pprint(list_ledger_entries(child_id=args.child_id))
        return

    if args.command == "adjust-balance":
        try:
            entry_id = add_manual_ledger_entry(args.child_id, args.asset, args.amount, args.note)
        except ValueError as err:
            parser.error(str(err))
        print(f"Ledger adjustment recorded with id={entry_id}")
        return

    if args.command == "redeem-reward":
        try:
            entry_id = redeem_reward(args.child_id, args.reward_id, args.note)
        except ValueError as err:
            parser.error(str(err))
        print(f"Reward redemption recorded with ledger entry id={entry_id}")
        return

    if args.command == "complete-task":
        try:
            if args.instance_id:
                completion_id = submit_task_instance(args.instance_id, args.note)
            else:
                if not args.task_id:
                    parser.error("Either --task-id or --instance-id is required")
                if args.child_id is None:
                    parser.error("--child-id is required when using --task-id")
                completion_id = record_task_completion(args.child_id, args.task_id, args.note)
        except ValueError as err:
            parser.error(str(err))
        print(f"Completion recorded with id={completion_id} (pending review)")
        return

    if args.command == "set-child-pin":
        try:
            updated = set_child_pin(args.child_id, args.pin)
        except ValueError as err:
            parser.error(str(err))
        if not updated:
            parser.error("Child not found")
        print(f"PIN set for child id={args.child_id}")
        return

    if args.command == "list-completions":
        pprint(list_task_completions(status=args.status, child_id=args.child_id))
        return

    if args.command == "review-completion":
        updated = review_task_completion(args.id, args.decision, args.by, args.note)
        if not updated:
            parser.error("Completion not found or no longer pending")
        print(f"Completion id={args.id} marked as {args.decision}")
        return

    parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
