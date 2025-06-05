from core.column import Column

def test_primary_key_and_unique():
    col = Column("id", "INT", ["PRIMARY KEY"])
    assert col.is_primary()
    assert col.is_unique()
    assert col.is_not_null()

def test_not_null():
    col = Column("email", "TEXT", ["NOT NULL"])
    assert not col.is_primary()
    assert not col.is_unique()
    assert col.is_not_null()
