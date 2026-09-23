-- Executed automatically on the container's first start.
CREATE TABLE source_table (
    id      INT PRIMARY KEY,
    payload TEXT NOT NULL
);

INSERT INTO source_table (id, payload) VALUES
    (1, 'hello'),
    (2, 'from'),
    (3, 'postgres');
