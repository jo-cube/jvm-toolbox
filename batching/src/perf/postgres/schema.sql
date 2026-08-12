CREATE TABLE IF NOT EXISTS lookup_value (
    key_text text NOT NULL,
    key_number integer NOT NULL,
    value numeric(20, 2) NOT NULL,
    PRIMARY KEY (key_text, key_number)
);
