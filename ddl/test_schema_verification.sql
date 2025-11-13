CREATE TABLE public.test_schema_verification (
    id INTEGER NOT NULL distkey,
    test_name VARCHAR(255) NOT NULL,
    test_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    test_status VARCHAR(50),
    new_test_column VARCHAR(500),
    another_test_field SUPER
)
DISTSTYLE KEY
SORTKEY(test_date);
-- Added new columns for testing schema migration
