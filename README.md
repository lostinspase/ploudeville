# Family Task and Reward System

Local-first project for managing how children earn rewards through required and optional tasks.

## Goals
- Keep task and reward definitions in a database for easy updates.
- Support required tasks (for allowance) and optional tasks (for screen-time and other rewards).
- Build as modular services so multiple functions can run and evolve in parallel.

## Initial Architecture
- `src/family_system/db.py`: SQLite connection and schema setup.
- `src/family_system/models.py`: domain enums and row mapping helpers.
- `src/family_system/repository.py`: CRUD operations for tasks/rewards/children.
- `src/family_system/cli.py`: command-line interface for maintenance.
- `scripts/init_db.py`: bootstrap local database.
- `data/`: SQLite file storage.

## Data Model (v1)
- `children`: who can do tasks and earn rewards.
- `tasks`: required vs optional, point value, payout type.
- `rewards`: reward catalog, including allowance and screen-time options.
- `task_schedules`: recurring daily/weekly scheduling rules.
- `task_instances`: generated due-task entries per child/date.
- `task_completions`: child submissions with parent approval status.
- `ledger_entries`: wallet transactions for allowance, screen-time, and points.
- `pet_species`: companion catalog (including Unicorn and Alacorn).
- `pet_adoptions`: each child's weekly chosen pet and name.
- `pet_care`: weekly feed/water/nurture tracking.
- `pet_badges`: weekly badge awards and streak achievements.
- `messages`: shared message feed for parents, kids, and pets.
- `charities`: approved and child-suggested charities with website/tax-exempt verification fields.
- `donation_pledges`: child donation requests completed by parents once the actual donation is made.
- `wallet_payouts`: child allowance payout requests, parent transfer completion, and payout audit history.
- `service_organizations`: volunteer organizations kids can select (and add to) for service work.
- `service_entries`: service-hour submissions, parent review, and completion status.
- `child_house_rules`: per-child weekday/weekend screen-off and bedtime defaults.
- `motd_library` + `motd_schedule`: message-of-the-day library and per-date schedule.
- `fun_fact_library` + `fun_fact_schedule`: fun-fact library and per-date schedule.
- `child_activities` + `holidays`: recurring/specific child activities with holiday no-school handling.
- `activity_notification_log`: per-activity notification send log to prevent duplicates.
- `concert_goals`: per-child concert savings goals with 80/20 split math.
- `activity_notification_attempts`: notification send attempts with success/failure and error details.
- `birthday_events` + `parent_birthday_events`: one-time yearly birthday treatment tracking.

## Quick Start
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 scripts/init_db.py
python3 -m src.family_system.cli seed-family
python3 -m src.family_system.web --host 127.0.0.1 --port 8000
```

## Groq Reading Quiz Adapter
```bash
export READING_CHATBOT_API_URL="http://127.0.0.1:8000/api/v1/reading-chatbot"
export READING_CHATBOT_API_KEY="replace-with-local-shared-key"
export GROQ_API_KEY="replace-with-your-groq-key"
export GROQ_MODEL="llama-3.1-8b-instant"
```

Then open `http://127.0.0.1:8000` in a browser.
- `Home`: pick a child portal.
- `Home` also shows total adopted pet count.
- `Child Portal`: protected by child PIN if configured.
- `Child Portal`: includes a Pet Center to view available pets, adopted pets, badges/streaks, create custom pet species, and manage weekly pet care.
- `Parent Panel`: password protected; includes email reset flow to `jploude@gmail.com`.
- `Parent Panel`: set child PINs, add/edit/disable/delete schedules, filter inactive schedules, approve submissions, add tasks/rewards, adjust balances, manage pets, and post messages.
- `Child Portal` and `Parent Panel`: include charity workflow (child suggestions, website check status, parent tax-exempt verification, and parent-completed donation requests).
- `Child Portal` and `Parent Panel`: include wallet payout workflow (request from allowance, parent marks sent to Apple Cash, transfer reference audit trail).
- `Child Portal` and `Parent Panel`: include service-hours workflow with organization selection, parent review, lifetime service-hour tracking, and configurable credit rates (allowance + screen time per hour).
- `Home` shows family schedule defaults per child and after-school reminders; parent panel can edit both.
- `Home` shows Message of the Day and Fun Fact of the Day; parent panel can add library entries and set either by date.
- `Home` and child portals show daily activities; parent panel can add child activities and holidays.
- Parent panel can configure activity notifications (enable, channel email/sms/both, minutes before start).
- Parent panel supports Ticketmaster search/import for concert goals (kid saves 80% of low price, parent covers 20%).
- Parent panel supports Adventure Park goals with seeded Southern California + Eastern Pennsylvania parks (kid saves 80% of low price, parent covers 20%).
- Daily wallet interest can be configured in Parent Panel and is auto-accrued once per child per day on positive allowance balances.
- Parent panel supports concert goal lifecycle states (`active`, `purchased`, `archived`), notification delivery logs, and one-click exports (JSON/CSV).

## iOS Sync Endpoint
- `POST /api/v1/ios/usage-sync`
- Optional auth: `Authorization: Bearer <IOS_SYNC_TOKEN>` when `IOS_SYNC_TOKEN` is configured.
- JSON body:
  - `childName` (string)
  - `date` (`YYYY-MM-DD`)
  - `totalMinutes` (int)
  - `perAppMinutes` (object `{appName: minutes}`)
- `POST /api/v1/ios/safety-alert`
- Optional auth: same `Authorization: Bearer <IOS_SYNC_TOKEN>` behavior.
- JSON body:
  - `childName` (string)
  - `date` (`YYYY-MM-DD`)
  - `severity` (`info|warning|critical`)
  - `reason` (string)
  - `notifyParentsEmail` (bool)
  - `notifyParentsSms` (bool)

## Notification Env Vars
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM` for email notifications.
- Apeiron SMS auth supports either:
  - `APEIRON_SMS_USER` + `APEIRON_SMS_TOKEN` (Basic Auth)
  - `APEIRON_API_KEY` (Bearer token fallback)
- `APEIRON_SMS_FROM` optional sender number.
- `APEIRON_SMS_ENDPOINT` optional override (default `https://api.apeiron.io/sms/send`).
- `TICKETMASTER_API_KEY` enables concert event search data in parent panel.
- `IOS_SYNC_TOKEN` optional bearer token for securing iOS usage sync endpoint.
- `READING_CHATBOT_API_URL` optional endpoint for reading quiz generation/evaluation.
- `READING_CHATBOT_API_KEY` optional bearer token for reading chatbot endpoint auth.
- `GROQ_API_KEY` enables the built-in reading-chatbot adapter endpoint.
- `GROQ_MODEL` optional Groq model override (default `llama-3.1-8b-instant`).
- `GROQ_API_URL` optional Groq endpoint override (default `https://api.groq.com/openai/v1/chat/completions`).
- Parent panel supports child contact info (email/text number) and can email child PIN reset links.
- Daily screen-time allotment is tracked and displayed: default 60 min weekdays, 120 min weekends.
- Child portals include per-app limits (seeded: Roblox 60 min/day for Elliana/Gracelyn, 0 for Rosie) and parent can add/update app limits.
- Child portals include Reading Log + 2-question quiz; credit is awarded only after quiz pass.
- Kids can adopt a weekly pet once they hit minimum weekly required-task completions, care for pets (feed/water/nurture), and see pet help alerts.
- Birthday special-day treatment is auto-applied once per year for configured birthdays (kids + parents) and surfaces in messages/GUI.

## Example Commands
```bash
# Add a child
python3 -m src.family_system.cli add-child --name "Elliana" --age 10

# Seed the default family children
python3 -m src.family_system.cli seed-family

# Add required task (money/allowance)
python3 -m src.family_system.cli add-task \
  --name "Unload dishwasher" \
  --rank required \
  --payout allowance \
  --value 2.00

# Add optional task (screen-time)
python3 -m src.family_system.cli add-task \
  --name "Practice piano" \
  --rank optional \
  --payout screen_time \
  --value 15

# Set child PIN (kid mode)
python3 -m src.family_system.cli set-child-pin --child-id 1 --pin 1234

# Add recurring schedule (daily for child 1)
python3 -m src.family_system.cli add-schedule \
  --task-id 1 \
  --cadence daily \
  --child-id 1 \
  --due-time 18:00

# Add recurring schedule (weekly for all children on Saturday, day 5)
python3 -m src.family_system.cli add-schedule \
  --task-id 2 \
  --cadence weekly \
  --day-of-week 5

# Generate due tasks for next 14 days
python3 -m src.family_system.cli generate-instances --days 14

# List due tasks for a child today
python3 -m src.family_system.cli list-due-tasks --child-id 1

# Add reward
python3 -m src.family_system.cli add-reward \
  --name "30 minutes gaming" \
  --type screen_time \
  --cost 30

# Child marks task complete (pending parent review)
python3 -m src.family_system.cli complete-task \
  --instance-id 1 \
  --note "Done before dinner"

# Parent reviews completion
python3 -m src.family_system.cli review-completion \
  --id 1 \
  --decision approved \
  --by "Mom"

# View completion records
python3 -m src.family_system.cli list-completions --status pending

# View balances by child and asset
python3 -m src.family_system.cli list-balances

# Manual adjustment (+/-) for a child wallet
python3 -m src.family_system.cli adjust-balance \
  --child-id 1 \
  --asset allowance \
  --amount 5 \
  --note "Weekly bonus"

# Redeem a reward (deducts from matching asset wallet)
python3 -m src.family_system.cli redeem-reward \
  --child-id 1 \
  --reward-id 1

# View full ledger history
python3 -m src.family_system.cli list-ledger --child-id 1
```

## Next Steps
- Add bulk schedule actions (multi-select enable/disable/delete).
- Add simple web UI and auth for family members.
