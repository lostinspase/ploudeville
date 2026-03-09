from pathlib import Path
import sqlite3
from itertools import product

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "family.db"


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def _build_seed_motd_messages() -> list[str]:
    starters = [
        "Today is a good day to",
        "You are strong enough to",
        "Your kind heart can",
        "Your brave voice can",
        "Your steady effort can",
        "Your smile can",
        "Your thoughtful choices can",
        "Your calm mind can",
        "Your curiosity can",
        "Your leadership can",
    ]
    actions = [
        "learn something new",
        "help someone feel included",
        "show kindness first",
        "keep going even when it is hard",
        "make a wise choice",
        "stand up for what is right",
        "practice with patience",
        "finish what you start",
        "listen with respect",
        "speak with confidence",
    ]
    closers = [
        "and that matters.",
        "and your family is proud of you.",
        "and make your community better.",
        "and build real confidence.",
        "and grow into who you are meant to be.",
        "and spread good citizenship.",
        "and make today meaningful.",
        "and inspire the people around you.",
        "and show your character.",
        "and create something beautiful.",
    ]
    messages: list[str] = []
    for a, b, c in product(starters, actions, closers):
        messages.append(f"{a} {b}, {c}")
        if len(messages) >= 220:
            break
    return messages


def _build_seed_fun_facts() -> list[str]:
    prefixes = [
        "Wild fact:",
        "Weird fact:",
        "Awesome fact:",
        "Nature fact:",
        "World fact:",
    ]
    facts = [
        "Octopuses have three hearts.",
        "Honey never spoils when stored well.",
        "A group of flamingos is called a flamboyance.",
        "Bananas are berries, but strawberries are not.",
        "Sea otters hold hands while floating so they do not drift apart.",
        "Wombats make cube-shaped poop.",
        "Sharks are older than trees in Earth's history.",
        "An ostrich eye is larger than its brain.",
        "A bolt of lightning is hotter than the surface of the Sun.",
        "Sloths can hold their breath longer than dolphins.",
        "Some turtles can breathe through their rear ends in cold seasons.",
        "A day on Venus is longer than a year on Venus.",
        "Koalas have fingerprints similar to humans.",
        "The Eiffel Tower can grow taller in summer heat.",
        "Elephants can recognize themselves in mirrors.",
        "A group of crows is called a murder.",
        "Pineapples take around two years to grow.",
        "Rainbows are full circles, but we usually see only arcs.",
        "Jellyfish have existed for hundreds of millions of years.",
        "The shortest war in history lasted less than an hour.",
        "Bamboo is one of the fastest-growing plants on Earth.",
        "Cows have best friends and can get stressed when separated.",
        "Some frogs can freeze and thaw back to life.",
        "The human nose can detect over a trillion scent combinations.",
        "A teaspoon of neutron-star material would weigh billions of tons.",
        "There are more stars in the universe than grains of sand on Earth.",
        "Bees can communicate flower locations by dancing.",
        "The Pacific Ocean is larger than all land on Earth combined.",
        "Water can boil and freeze at the same time under special conditions.",
        "Polar bears have black skin under white-looking fur.",
        "A group of owls is called a parliament.",
        "Saturn could float in water if you had a tub big enough.",
        "Some cats are allergic to people.",
        "There are deserts in Antarctica.",
        "Hummingbirds can fly backward.",
        "The Moon is moving away from Earth a little each year.",
        "A crocodile cannot stick out its tongue.",
        "Sunflowers can help clean certain toxins from soil.",
        "Your body has enough iron to make a small nail.",
        "Hot water can freeze faster than cold water in some conditions.",
        "A single cloud can weigh over a million pounds.",
        "The largest living structure on Earth is the Great Barrier Reef.",
        "Some butterflies can taste with their feet.",
        "The tallest trees can be taller than a football field is long.",
        "A giraffe and a human have the same number of neck bones.",
        "The heart of a blue whale is about the size of a small car.",
        "Ravens can solve puzzles and use tools.",
        "Earth's core is about as hot as the Sun's surface.",
        "An apple, potato, and onion can taste similar if you smell nothing.",
        "Some penguins propose with pebbles.",
        "A sneeze can travel faster than highway speeds.",
        "Water bears can survive extreme environments.",
        "The Amazon rainforest helps create its own rain.",
        "A day on Mercury lasts about two Mercury years.",
        "Some mushrooms glow in the dark.",
        "A group of porcupines is called a prickle.",
        "The deepest ocean point is deeper than Mount Everest is tall.",
        "The first oranges were green in some regions.",
        "A bolt of lightning can branch into many paths at once.",
        "Camels have three eyelids to protect against sand.",
        "Some fish can climb waterfalls.",
        "Kangaroos cannot walk backward easily.",
        "Penguins have knees; they are just hidden by feathers.",
        "There are trees that are older than many civilizations.",
        "The Milky Way galaxy is still moving through space.",
        "A clouded leopard can climb down trees headfirst.",
        "Some seeds can sleep for years before sprouting.",
        "Moonquakes happen on the Moon.",
        "A single lightning flash can be seen from space.",
        "Some corals are animals, not plants.",
        "You can hear rhubarb leaves grow if you use sensitive microphones.",
        "Spiders can spin silk stronger than steel by weight.",
        "The Sahara used to be much greener long ago.",
        "A snail can sleep for long periods in dry weather.",
    ]
    messages: list[str] = []
    for i in range(365):
        prefix = prefixes[i % len(prefixes)]
        fact = facts[i % len(facts)]
        messages.append(f"{prefix} {fact} (Day {i + 1})")
    return messages


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS children (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                age INTEGER NOT NULL CHECK(age >= 0),
                birthdate TEXT,
                email TEXT,
                text_number TEXT,
                pin_hash TEXT,
                default_pet_species_id INTEGER REFERENCES pet_species(id),
                default_pet_name TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                rank TEXT NOT NULL CHECK(rank IN ('required', 'optional')),
                payout_type TEXT NOT NULL CHECK(payout_type IN ('allowance', 'screen_time', 'points')),
                payout_value REAL NOT NULL CHECK(payout_value >= 0),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS rewards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                reward_type TEXT NOT NULL CHECK(reward_type IN ('allowance', 'screen_time', 'privilege')),
                cost REAL NOT NULL CHECK(cost >= 0),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS task_completions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                payout_type TEXT NOT NULL CHECK(payout_type IN ('allowance', 'screen_time', 'points')),
                payout_value REAL NOT NULL CHECK(payout_value >= 0),
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'approved', 'rejected')),
                completion_note TEXT NOT NULL DEFAULT '',
                completed_at TEXT NOT NULL DEFAULT (datetime('now')),
                reviewed_by TEXT,
                review_note TEXT NOT NULL DEFAULT '',
                reviewed_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                asset_type TEXT NOT NULL CHECK(asset_type IN ('allowance', 'screen_time', 'points')),
                amount REAL NOT NULL CHECK(amount != 0),
                source_type TEXT NOT NULL CHECK(source_type IN ('task_completion', 'reward_redemption', 'manual_adjustment')),
                task_completion_id INTEGER REFERENCES task_completions(id),
                reward_id INTEGER REFERENCES rewards(id),
                note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(task_completion_id)
            );

            CREATE TABLE IF NOT EXISTS task_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                child_id INTEGER REFERENCES children(id),
                cadence TEXT NOT NULL CHECK(cadence IN ('daily', 'weekly')),
                day_of_week INTEGER CHECK(day_of_week BETWEEN 0 AND 6),
                period_mode TEXT NOT NULL DEFAULT 'day_of_week' CHECK(period_mode IN ('day_of_week', 'all_days', 'times_per_period')),
                times_per_period INTEGER NOT NULL DEFAULT 1 CHECK(times_per_period BETWEEN 1 AND 7),
                due_time TEXT,
                plan_scope TEXT NOT NULL DEFAULT 'standard' CHECK(plan_scope IN ('standard', 'weekly_allowance_default', 'weekly_allowance_override')),
                week_key TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS task_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL REFERENCES task_schedules(id),
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                child_id INTEGER NOT NULL REFERENCES children(id),
                due_date TEXT NOT NULL,
                due_time TEXT,
                status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open', 'submitted', 'approved', 'rejected')),
                task_completion_id INTEGER REFERENCES task_completions(id),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(task_id, child_id, due_date)
            );

            CREATE TABLE IF NOT EXISTS pet_species (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                rarity TEXT NOT NULL DEFAULT 'common',
                is_custom INTEGER NOT NULL DEFAULT 0,
                created_by_child_id INTEGER REFERENCES children(id),
                active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS pet_adoptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                week_key TEXT NOT NULL,
                pet_species_id INTEGER NOT NULL REFERENCES pet_species(id),
                pet_name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, week_key)
            );

            CREATE TABLE IF NOT EXISTS pet_care (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                week_key TEXT NOT NULL,
                care_type TEXT NOT NULL CHECK(care_type IN ('feed', 'water', 'nurture')),
                completed_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, week_key, care_type)
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_type TEXT NOT NULL CHECK(sender_type IN ('parent', 'kid', 'pet', 'system')),
                sender_name TEXT NOT NULL,
                child_id INTEGER REFERENCES children(id),
                message_text TEXT NOT NULL,
                week_key TEXT,
                message_kind TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, message_kind, week_key)
            );

            CREATE TABLE IF NOT EXISTS pet_badges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                week_key TEXT NOT NULL,
                badge_code TEXT NOT NULL,
                badge_name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, week_key, badge_code)
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS parent_reset_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS child_pin_reset_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                token TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS birthday_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                year INTEGER NOT NULL,
                allowance_bonus REAL NOT NULL,
                screen_time_bonus REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, year)
            );

            CREATE TABLE IF NOT EXISTS parents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                text_number TEXT,
                birthdate TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS parent_birthday_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER NOT NULL REFERENCES parents(id),
                year INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(parent_id, year)
            );

            CREATE TABLE IF NOT EXISTS charities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                website TEXT NOT NULL,
                ein TEXT,
                website_live INTEGER NOT NULL DEFAULT 0,
                tax_exempt_verified INTEGER NOT NULL DEFAULT 0,
                verified_by_parent TEXT,
                created_by_child_id INTEGER REFERENCES children(id),
                seed_charity INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS donation_pledges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                charity_id INTEGER NOT NULL REFERENCES charities(id),
                amount REAL NOT NULL CHECK(amount > 0),
                note TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending_parent' CHECK(status IN ('pending_parent', 'completed')),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                completed_at TEXT,
                completed_by TEXT
            );

            CREATE TABLE IF NOT EXISTS wallet_payouts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                amount REAL NOT NULL CHECK(amount > 0),
                note TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending_parent' CHECK(status IN ('pending_parent', 'sent', 'cancelled')),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                sent_at TEXT,
                sent_by TEXT,
                transfer_reference TEXT
            );

            CREATE TABLE IF NOT EXISTS weekly_allowance_settings (
                child_id INTEGER PRIMARY KEY REFERENCES children(id),
                default_amount REAL NOT NULL DEFAULT 0 CHECK(default_amount >= 0),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS weekly_allowance_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                week_key TEXT NOT NULL,
                allowance_amount REAL NOT NULL CHECK(allowance_amount >= 0),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, week_key)
            );

            CREATE TABLE IF NOT EXISTS weekly_allowance_credits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                week_key TEXT NOT NULL,
                allowance_amount REAL NOT NULL CHECK(allowance_amount >= 0),
                ledger_entry_id INTEGER REFERENCES ledger_entries(id),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, week_key)
            );

            CREATE TABLE IF NOT EXISTS reading_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                read_date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                book_title TEXT NOT NULL,
                chapters TEXT NOT NULL,
                question_1 TEXT NOT NULL DEFAULT '',
                question_2 TEXT NOT NULL DEFAULT '',
                answer_1 TEXT NOT NULL DEFAULT '',
                answer_2 TEXT NOT NULL DEFAULT '',
                score REAL NOT NULL DEFAULT 0,
                passed INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending_questions' CHECK(status IN ('pending_questions', 'awaiting_answers', 'failed', 'passed')),
                chatbot_provider TEXT NOT NULL DEFAULT '',
                parent_override_by TEXT,
                parent_override_at TEXT,
                credit_completion_id INTEGER REFERENCES task_completions(id),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                evaluated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS interest_accruals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                accrual_date TEXT NOT NULL,
                rate_percent REAL NOT NULL CHECK(rate_percent >= 0),
                opening_balance REAL NOT NULL CHECK(opening_balance >= 0),
                interest_amount REAL NOT NULL CHECK(interest_amount >= 0),
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, accrual_date)
            );

            CREATE TABLE IF NOT EXISTS service_organizations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                website TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_by_child_id INTEGER REFERENCES children(id),
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS service_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                organization_id INTEGER NOT NULL REFERENCES service_organizations(id),
                hours REAL NOT NULL CHECK(hours > 0),
                service_date TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending_parent' CHECK(status IN ('pending_parent', 'completed', 'rejected')),
                reviewed_by TEXT,
                review_note TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                reviewed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS child_house_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL UNIQUE REFERENCES children(id),
                weekday_screen_off TEXT NOT NULL,
                weekday_bedtime TEXT NOT NULL,
                weekend_screen_off TEXT NOT NULL,
                weekend_bedtime TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS motd_library (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL DEFAULT 'seed',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS motd_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_date TEXT NOT NULL UNIQUE,
                message_text TEXT NOT NULL,
                library_id INTEGER REFERENCES motd_library(id),
                set_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS fun_fact_library (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fact_text TEXT NOT NULL UNIQUE,
                category TEXT NOT NULL DEFAULT 'seed',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS fun_fact_schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_date TEXT NOT NULL UNIQUE,
                fact_text TEXT NOT NULL,
                library_id INTEGER REFERENCES fun_fact_library(id),
                set_by TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS child_activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                activity_name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'activity',
                day_of_week INTEGER CHECK(day_of_week BETWEEN 0 AND 6),
                specific_date TEXT,
                start_time TEXT NOT NULL,
                end_time TEXT,
                location TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                notify_enabled INTEGER NOT NULL DEFAULT 0,
                notify_minutes_before INTEGER NOT NULL DEFAULT 30,
                notify_channels TEXT NOT NULL DEFAULT 'email',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS holidays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                holiday_date TEXT NOT NULL UNIQUE,
                holiday_name TEXT NOT NULL,
                no_school INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS activity_notification_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER NOT NULL REFERENCES child_activities(id),
                occurrence_date TEXT NOT NULL,
                channel TEXT NOT NULL CHECK(channel IN ('email', 'sms')),
                sent_to TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(activity_id, occurrence_date, channel)
            );

            CREATE TABLE IF NOT EXISTS activity_notification_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                activity_id INTEGER NOT NULL REFERENCES child_activities(id),
                occurrence_date TEXT NOT NULL,
                channel TEXT NOT NULL CHECK(channel IN ('email', 'sms')),
                target TEXT NOT NULL DEFAULT '',
                success INTEGER NOT NULL DEFAULT 0,
                error_text TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS child_app_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                app_name TEXT NOT NULL,
                minutes_per_day INTEGER NOT NULL CHECK(minutes_per_day >= 0),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(child_id, app_name)
            );

            CREATE TABLE IF NOT EXISTS concert_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                artist_name TEXT NOT NULL,
                event_name TEXT NOT NULL,
                event_date TEXT NOT NULL,
                venue_name TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                state_code TEXT NOT NULL DEFAULT '',
                low_price REAL NOT NULL CHECK(low_price >= 0),
                high_price REAL NOT NULL CHECK(high_price >= low_price),
                currency TEXT NOT NULL DEFAULT 'USD',
                ticket_url TEXT NOT NULL DEFAULT '',
                kid_share_percent INTEGER NOT NULL DEFAULT 80 CHECK(kid_share_percent BETWEEN 1 AND 100),
                goal_status TEXT NOT NULL DEFAULT 'active' CHECK(goal_status IN ('active', 'purchased', 'archived')),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS adventure_park_catalog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT 'theme_park',
                website TEXT NOT NULL DEFAULT '',
                low_price REAL NOT NULL CHECK(low_price >= 0),
                high_price REAL NOT NULL CHECK(high_price >= low_price),
                currency TEXT NOT NULL DEFAULT 'USD',
                active INTEGER NOT NULL DEFAULT 1,
                seed_park INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(name, region)
            );

            CREATE TABLE IF NOT EXISTS adventure_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_id INTEGER NOT NULL REFERENCES children(id),
                park_name TEXT NOT NULL,
                ticket_name TEXT NOT NULL,
                region TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT 'theme_park',
                target_date TEXT NOT NULL,
                low_price REAL NOT NULL CHECK(low_price >= 0),
                high_price REAL NOT NULL CHECK(high_price >= low_price),
                currency TEXT NOT NULL DEFAULT 'USD',
                ticket_url TEXT NOT NULL DEFAULT '',
                kid_share_percent INTEGER NOT NULL DEFAULT 80 CHECK(kid_share_percent BETWEEN 1 AND 100),
                goal_status TEXT NOT NULL DEFAULT 'active' CHECK(goal_status IN ('active', 'purchased', 'archived')),
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS ios_usage_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                child_name TEXT NOT NULL,
                usage_date TEXT NOT NULL,
                total_minutes INTEGER NOT NULL CHECK(total_minutes >= 0),
                per_app_json TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'ios_companion',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            """
        )

        # Migration for early schema versions that did not include age.
        if not _column_exists(conn, "children", "age"):
            conn.execute("ALTER TABLE children ADD COLUMN age INTEGER NOT NULL DEFAULT 0")
        if not _column_exists(conn, "children", "birthdate"):
            conn.execute("ALTER TABLE children ADD COLUMN birthdate TEXT")
        if not _column_exists(conn, "children", "email"):
            conn.execute("ALTER TABLE children ADD COLUMN email TEXT")
        if not _column_exists(conn, "children", "text_number"):
            conn.execute("ALTER TABLE children ADD COLUMN text_number TEXT")
        if not _column_exists(conn, "parents", "text_number"):
            conn.execute("ALTER TABLE parents ADD COLUMN text_number TEXT")
        if not _column_exists(conn, "child_activities", "notify_enabled"):
            conn.execute("ALTER TABLE child_activities ADD COLUMN notify_enabled INTEGER NOT NULL DEFAULT 0")
        if not _column_exists(conn, "child_activities", "notify_minutes_before"):
            conn.execute("ALTER TABLE child_activities ADD COLUMN notify_minutes_before INTEGER NOT NULL DEFAULT 30")
        if not _column_exists(conn, "child_activities", "notify_channels"):
            conn.execute("ALTER TABLE child_activities ADD COLUMN notify_channels TEXT NOT NULL DEFAULT 'email'")
        if not _column_exists(conn, "concert_goals", "goal_status"):
            conn.execute("ALTER TABLE concert_goals ADD COLUMN goal_status TEXT NOT NULL DEFAULT 'active'")
        if not _column_exists(conn, "adventure_goals", "goal_status"):
            conn.execute("ALTER TABLE adventure_goals ADD COLUMN goal_status TEXT NOT NULL DEFAULT 'active'")
        if not _column_exists(conn, "children", "pin_hash"):
            conn.execute("ALTER TABLE children ADD COLUMN pin_hash TEXT")
        if not _column_exists(conn, "children", "default_pet_species_id"):
            conn.execute("ALTER TABLE children ADD COLUMN default_pet_species_id INTEGER REFERENCES pet_species(id)")
        if not _column_exists(conn, "children", "default_pet_name"):
            conn.execute("ALTER TABLE children ADD COLUMN default_pet_name TEXT")
        if not _column_exists(conn, "task_completions", "task_instance_id"):
            conn.execute("ALTER TABLE task_completions ADD COLUMN task_instance_id INTEGER REFERENCES task_instances(id)")
        if not _column_exists(conn, "task_schedules", "plan_scope"):
            conn.execute("ALTER TABLE task_schedules ADD COLUMN plan_scope TEXT NOT NULL DEFAULT 'standard'")
        if not _column_exists(conn, "task_schedules", "week_key"):
            conn.execute("ALTER TABLE task_schedules ADD COLUMN week_key TEXT")
        if not _column_exists(conn, "task_schedules", "period_mode"):
            conn.execute("ALTER TABLE task_schedules ADD COLUMN period_mode TEXT NOT NULL DEFAULT 'day_of_week'")
        if not _column_exists(conn, "task_schedules", "times_per_period"):
            conn.execute("ALTER TABLE task_schedules ADD COLUMN times_per_period INTEGER NOT NULL DEFAULT 1")
        if not _column_exists(conn, "pet_species", "is_custom"):
            conn.execute("ALTER TABLE pet_species ADD COLUMN is_custom INTEGER NOT NULL DEFAULT 0")
        if not _column_exists(conn, "pet_species", "created_by_child_id"):
            conn.execute("ALTER TABLE pet_species ADD COLUMN created_by_child_id INTEGER REFERENCES children(id)")
        if not _column_exists(conn, "wallet_payouts", "transfer_reference"):
            conn.execute("ALTER TABLE wallet_payouts ADD COLUMN transfer_reference TEXT")
        if not _column_exists(conn, "reading_logs", "parent_override_by"):
            conn.execute("ALTER TABLE reading_logs ADD COLUMN parent_override_by TEXT")
        if not _column_exists(conn, "reading_logs", "parent_override_at"):
            conn.execute("ALTER TABLE reading_logs ADD COLUMN parent_override_at TEXT")

        conn.executemany(
            """
            INSERT OR IGNORE INTO pet_species (name, rarity, active)
            VALUES (?, ?, 1)
            """,
            [
                ("Unicorn", "mythic"),
                ("Alacorn", "legendary"),
                ("Axolotl Knight", "rare"),
                ("Space Llama", "rare"),
                ("Thunder Capybara", "rare"),
                ("Glow Gecko", "common"),
                ("Cloud Otter", "uncommon"),
                ("Goblin Hamster", "uncommon"),
                ("Disco Penguin", "uncommon"),
                ("Dragonfruit Sloth", "rare"),
                ("Moon Jellycat", "rare"),
                ("Marshmallow Yeti", "mythic"),
            ],
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO app_settings (key, value)
            VALUES (?, ?)
            """,
            [
                ("parent_password_hash", "414cb7615ce407f27f3231eadef7b6e025bf9298f46336ea7bcf690f7077f8ce"),
                ("parent_reset_email", "jploude@gmail.com"),
                ("service_allowance_per_hour", "0"),
                ("service_screen_minutes_per_hour", "0"),
                ("after_school_reminders", "homework,practice voice,practice piano,exercise,read"),
                ("screen_allot_weekday_minutes", "60"),
                ("screen_allot_weekend_minutes", "120"),
                ("wallet_daily_interest_rate_percent", "0.05"),
            ],
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO parents (name, email, birthdate, active)
            VALUES (?, ?, ?, 1)
            """,
            [
                ("JP", "jploude@gmail.com", None),
                ("Tiffany", "tleighbaldwin@gmail.com", "1979-11-30"),
            ],
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO charities (
                name, website, ein, website_live, tax_exempt_verified, seed_charity
            )
            VALUES (?, ?, ?, 1, 1, 1)
            """,
            [
                ("The Nature Conservancy", "https://www.nature.org", "53-0242652"),
                ("charity: water", "https://www.charitywater.org", "22-3936753"),
                ("National Alliance to End Homelessness", "https://endhomelessness.org", "52-1299641"),
                ("St. Jude Children's Research Hospital", "https://www.stjude.org", "62-0646012"),
                ("Direct Relief", "https://www.directrelief.org", "95-1831116"),
            ],
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO service_organizations (name, website, active)
            VALUES (?, ?, 1)
            """,
            [
                ("The Salvation Army (Soup Kitchen & Pantry)", "https://www.salvationarmyusa.org/volunteer/"),
                ("Surfrider Foundation (Beach Cleanup)", "https://www.surfrider.org/"),
                ("Habitat for Humanity", "https://www.habitat.org/volunteer/near-you"),
                ("Feeding America Network Food Banks", "https://www.feedingamerica.org/take-action/volunteer"),
                ("Ronald McDonald House Charities", "https://ronaldmcdonaldhouse.org/get-involved/volunteer"),
            ],
        )

        conn.executemany(
            """
            INSERT OR IGNORE INTO adventure_park_catalog (
                name, region, category, website, low_price, high_price, currency, active, seed_park
            )
            VALUES (?, ?, ?, ?, ?, ?, 'USD', 1, 1)
            """,
            [
                ("Disneyland Park", "Southern California", "theme_park", "https://disneyland.disney.go.com", 104.0, 194.0),
                ("Disney California Adventure", "Southern California", "theme_park", "https://disneyland.disney.go.com", 104.0, 194.0),
                ("Knott's Berry Farm", "Southern California", "theme_park", "https://www.knotts.com", 55.0, 120.0),
                ("Knott's Soak City", "Southern California", "water_park", "https://www.knotts.com/soak-city", 45.0, 95.0),
                ("Great Wolf Lodge Water Park (SoCal)", "Southern California", "water_park", "https://www.greatwolf.com/southern-california", 50.0, 110.0),
                ("Dorney Park", "Eastern Pennsylvania", "theme_park", "https://www.dorneypark.com", 45.0, 95.0),
                ("Camelbeach Outdoor Waterpark", "Eastern Pennsylvania", "water_park", "https://www.camelbackresort.com", 40.0, 85.0),
                ("Hersheypark", "Eastern Pennsylvania", "theme_park", "https://www.hersheypark.com", 60.0, 120.0),
                ("Sesame Place Philadelphia", "Eastern Pennsylvania", "theme_park", "https://sesameplace.com/philadelphia", 55.0, 110.0),
            ],
        )

        # One-time cleanup for earlier placeholder service org seeds.
        conn.executemany(
            """
            UPDATE service_organizations
            SET active = 0
            WHERE name = ?
              AND created_by_child_id IS NULL
            """,
            [
                ("The Nature Conservancy",),
                ("charity: water",),
                ("National Alliance to End Homelessness",),
                ("St. Jude Children's Research Hospital",),
                ("Direct Relief",),
            ],
        )

        # Default child schedule rules (only inserted when missing).
        conn.executescript(
            """
            INSERT OR IGNORE INTO child_house_rules (
                child_id, weekday_screen_off, weekday_bedtime, weekend_screen_off, weekend_bedtime
            )
            SELECT id, '20:15', '21:00', '21:00', '21:30'
            FROM children
            WHERE name IN ('Elliana', 'Gracelyn', 'Gracie');

            INSERT OR IGNORE INTO child_house_rules (
                child_id, weekday_screen_off, weekday_bedtime, weekend_screen_off, weekend_bedtime
            )
            SELECT id, '19:45', '20:00', '20:15', '20:30'
            FROM children
            WHERE name = 'Rosie';
            """
        )

        motd_messages = _build_seed_motd_messages()
        conn.executemany(
            """
            INSERT OR IGNORE INTO motd_library (message_text, category, active)
            VALUES (?, 'seed', 1)
            """,
            [(m,) for m in motd_messages],
        )
        fun_facts = _build_seed_fun_facts()
        conn.executemany(
            """
            INSERT OR IGNORE INTO fun_fact_library (fact_text, category, active)
            VALUES (?, 'seed', 1)
            """,
            [(f,) for f in fun_facts],
        )

        # Default school/activity schedules by child.
        conn.executescript(
            """
            -- Elliana school schedule
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 0, '08:30', '14:30'
            FROM children c
            WHERE c.name = 'Elliana'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 0 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 1, '08:30', '14:30'
            FROM children c
            WHERE c.name = 'Elliana'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 1 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 2, '08:30', '12:30'
            FROM children c
            WHERE c.name = 'Elliana'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 2 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 3, '08:30', '14:30'
            FROM children c
            WHERE c.name = 'Elliana'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 3 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 4, '08:30', '14:30'
            FROM children c
            WHERE c.name = 'Elliana'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 4 AND COALESCE(a.specific_date, '') = ''
              );

            -- Gracelyn/Gracie school schedule
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 0, '08:30', '14:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 0 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 1, '08:30', '14:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 1 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 2, '08:30', '12:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 2 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 3, '08:30', '14:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 3 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 4, '08:30', '14:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 4 AND COALESCE(a.specific_date, '') = ''
              );

            -- Rosie school schedule
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 0, '08:00', '16:30'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 0 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 1, '08:00', '16:30'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 1 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 2, '08:00', '16:30'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 2 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 3, '08:00', '16:30'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 3 AND COALESCE(a.specific_date, '') = ''
              );
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time, end_time)
            SELECT c.id, 'School', 'school', 4, '08:00', '16:30'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'School' AND a.day_of_week = 4 AND COALESCE(a.specific_date, '') = ''
              );

            -- Rosie soccer Saturdays at 9:45
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time)
            SELECT c.id, 'Soccer', 'sports', 5, '09:45'
            FROM children c
            WHERE c.name = 'Rosie'
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'Soccer' AND a.day_of_week = 5 AND COALESCE(a.specific_date, '') = ''
              );

            -- Gracie gymnastics Mondays at 5:30
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time)
            SELECT c.id, 'Gymnastics', 'activity', 0, '17:30'
            FROM children c
            WHERE c.name IN ('Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'Gymnastics' AND a.day_of_week = 0 AND COALESCE(a.specific_date, '') = ''
              );

            -- Elliana & Gracelyn voice lessons Wednesdays at 5:00
            INSERT INTO child_activities (child_id, activity_name, category, day_of_week, start_time)
            SELECT c.id, 'Voice Lessons', 'activity', 2, '17:00'
            FROM children c
            WHERE c.name IN ('Elliana', 'Gracelyn', 'Gracie')
              AND NOT EXISTS (
                SELECT 1 FROM child_activities a
                WHERE a.child_id = c.id AND a.activity_name = 'Voice Lessons' AND a.day_of_week = 2 AND COALESCE(a.specific_date, '') = ''
              );

            UPDATE children
            SET email = 'eploude@icloud.com'
            WHERE name = 'Elliana' AND COALESCE(email, '') = '';

            UPDATE children
            SET email = 'gploude@icloud.com'
            WHERE name IN ('Gracelyn', 'Gracie') AND COALESCE(email, '') = '';

            UPDATE children
            SET email = 'rploude@icloud.com'
            WHERE name = 'Rosie' AND COALESCE(email, '') = '';

            INSERT OR IGNORE INTO child_app_limits (child_id, app_name, minutes_per_day, active)
            SELECT id, 'Roblox', 60, 1
            FROM children
            WHERE name IN ('Elliana', 'Gracelyn', 'Gracie');

            INSERT OR IGNORE INTO child_app_limits (child_id, app_name, minutes_per_day, active)
            SELECT id, 'Roblox', 0, 1
            FROM children
            WHERE name = 'Rosie';
            """
        )
