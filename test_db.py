import pytest
from Database import Database  # Update this with the actual name of your module

@pytest.fixture(scope="function")
def db():
    """Fixture to initialize and return a Database instance with a fresh test environment."""
    test_db = Database('test.db')  # Use a test database
    # Set up the test environment
    test_db.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT);")
    yield test_db
    # Tear down the test environment
    test_db.execute("DROP TABLE IF EXISTS test_table;")

def test_table_creation(db):
    """Test table creation."""
    db.execute("CREATE TABLE IF NOT EXISTS create_test (id INTEGER PRIMARY KEY, data TEXT);")
    # Verify the table was created by checking if we can insert and fetch from it
    db.execute("INSERT INTO create_test (data) VALUES ('test data');")
    result = db.fetchone("SELECT data FROM create_test WHERE id = 1;")
    assert result['data'] == 'test data', "Data inserted and fetched should match."
    # Clean up
    db.execute("DROP TABLE create_test;")

def test_insert_into_table(db):
    """Test data insertion."""
    db.execute("INSERT INTO test_table (name) VALUES ('John Doe');")
    result = db.fetchone("SELECT name FROM test_table WHERE id = 1;")
    assert result['name'] == 'John Doe', "The name of the inserted record should be 'John Doe'."

def test_delete_from_table(db):
    """Test record deletion."""
    # First, insert a record to ensure there is something to delete
    db.execute("CREATE TABLE IF NOT EXISTS create_test (id INTEGER PRIMARY KEY, data TEXT);")
    db.execute("INSERT INTO test_table (name) VALUES ('Jane Doe');")
    result = db.fetchone("SELECT name FROM test_table WHERE id = 1;")
    assert result['name'] == 'Jane Doe', "The name of the inserted record should be 'John Doe'."

    db.execute("DELETE FROM test_table WHERE name = 'Jane Doe';")
    result = db.fetchone("SELECT name FROM test_table WHERE name = 'Jane Doe';")
    # Clean up
    db.execute("DROP TABLE create_test;")
    assert result is None, "The record should have been deleted."


def test_create_schema(db):
    """Test Create schema."""
    db.create_schema()
    result = db.fetchall("SELECT name FROM sqlite_master where type='table';")
    assert set(['tickers', 'institutional_holdings', 'metadata']).issubset([d['name'] for d in result])
