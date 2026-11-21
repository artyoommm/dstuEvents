import sqlite3
import pytest
from shared.idempotency import ensure_table, get_cached, save, request_hash
from shared.problems import ProblemException


def _get_conn():
    # Создаем подключение и явно говорим ему возвращать Row (как в shared/database.py)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def test_roundtrip():
    conn = _get_conn()
    ensure_table(conn)
    h = request_hash({"a": 1})
    assert get_cached(conn, "k1", h) is None
    save(conn, "k1", h, '{"ok":true}', 200)
    assert get_cached(conn, "k1", h) == ('{"ok":true}', 200)


def test_mismatch():
    conn = _get_conn()
    ensure_table(conn)
    save(conn, "k1", "hashA", "{}", 200)
    with pytest.raises(ProblemException) as ei:
        get_cached(conn, "k1", "hashB")
    assert ei.value.problem.status == 422