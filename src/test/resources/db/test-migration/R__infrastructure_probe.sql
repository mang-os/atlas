-- Test-only proof of Flyway execution. Never included in the application JAR.
CREATE TABLE infrastructure_probe (
    id INTEGER PRIMARY KEY,
    description VARCHAR(80) NOT NULL
);
INSERT INTO infrastructure_probe (id, description) VALUES (1, 'Flyway executed');
