import pytest

from query.parser import parse_command
from query.querytype import QueryTypes, QueryType

@pytest.mark.parametrize("sql, expected_type", [
    ("CREATE TABLE users (id INT, name TEXT)", QueryTypes.CREATE),
    ("INSERT INTO users VALUES (1, 'Alice')", QueryTypes.INSERT),
    ("SELECT * FROM users", QueryTypes.SELECT),
])
def test_parse_command_type(sql, expected_type):
    result = parse_command(sql)
    assert result.type == expected_type

def test_parse_create_table():
    q = parse_command("CREATE TABLE test (id INT, name TEXT);")
    assert q.type == QueryTypes.CREATE
    assert q.table == "test"
    assert q.columns == [["id", "INT"], ["name", "TEXT"]]

def test_parse_create_other():
    # Should also work with CREATE INDEX or similar, but logic mirrors CREATE TABLE
    q = parse_command("CREATE INDEX idx (id, name);")
    assert q.type == QueryTypes.CREATE
    assert q.table == "idx"
    assert q.columns == [["id"], ["name"]]

def test_parse_insert():
    q = parse_command('INSERT INTO users VALUES (1, "Alice");')
    assert q.type == QueryTypes.INSERT
    assert q.table == "users"
    assert q.values == [1, "Alice"]

def test_parse_insert_int_and_string():
    q = parse_command('INSERT INTO test VALUES (42, \'Bob\');')
    assert q.type == QueryTypes.INSERT
    assert q.table == "test"
    assert q.values == [42, "Bob"]

def test_parse_select_all():
    q = parse_command("SELECT * FROM users;")
    assert q.type == QueryTypes.SELECT
    assert q.table == "users"
    assert q.conditions is None

def test_parse_select_with_where():
    q = parse_command("SELECT * FROM users WHERE id '42';")
    assert q.type == QueryTypes.SELECT
    assert q.table == "users"
    assert q.conditions == ("id", "42")

def test_parse_select_with_where_quotes():
    q = parse_command("SELECT * FROM users WHERE name 'Alice';")
    assert q.type == QueryTypes.SELECT
    assert q.table == "users"
    assert q.conditions == ("name", "Alice")

def test_parse_empty_command_returns_default():
    q = parse_command("")
    assert isinstance(q, QueryType)
    # By default, QueryType() might have no .type; adjust as needed.

def test_parse_unknown_command_prints(capsys):
    q = parse_command("DROP TABLE users;")
    captured = capsys.readouterr()
    assert "Couldn't parse query" in captured.out
    assert isinstance(q, QueryType)

def test_parse_unknown_command_returns_unknown():
    q = parse_command("DROP TABLE users;")
    assert q.type == QueryTypes.UNKNOWN

def test_parse_empty_command_returns_unknown()
    q = parse_command("")
    assert q.type == QueryTypes.UNKNOWN