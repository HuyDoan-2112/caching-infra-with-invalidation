-- Runs once, as root, when the MySQL data volume is empty.
-- The `app` user and its ALL PRIVILEGES on `catalog` come from MYSQL_USER / MYSQL_DATABASE.

CREATE DATABASE IF NOT EXISTS catalog
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE catalog;

CREATE TABLE IF NOT EXISTS items(
    id           BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(200) NOT NULL,
    description  VARCHAR(2000),
    price_cents  BIGINT       NOT NULL DEFAULT 0,
    currency     CHAR(3)      NOT NULL DEFAULT 'USD',
    stock        INT,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_items_updated_at (updated_at)
) ENGINE=InnoDB;

-- Debezium reads the binlog with its own user, so the services' `app` user
-- holds no replication privileges. Dev-only password.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES
  ON *.* TO 'debezium'@'%';
