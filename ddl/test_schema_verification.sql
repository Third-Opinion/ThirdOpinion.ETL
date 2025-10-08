CREATE TABLE public.test_schema_verification (
    id INTEGER NOT NULL,
    test_name VARCHAR(255) NOT NULL,
    test_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    test_status VARCHAR(50)
)
DISTSTYLE ALL
SORTKEY(test_date);
