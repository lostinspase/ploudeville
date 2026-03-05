from src.family_system.db import init_db


def test_db_init_runs() -> None:
    init_db()
    assert True
