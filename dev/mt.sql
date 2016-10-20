ALTER TABLE xmp_operators ADD COLUMN settings jsonb NOT NULL DEFAULT '{}';
UPDATE xmp_operators SET  settings = '{}' WHERE "name" = 'Mobilink';

ALTER TABLE xmp_services ADD COLUMN paid_hours INT NOT NULL DEFAULT 0;
UPDATE xmp_services SET paid_hours = 24 WHERE id = 777;

ALTER TABLE xmp_subscriptions ADD COLUMN paid transaction_statuses NOT NULL DEFAULT '';
UPDATE xmp_subscriptions SET paid = 'past' WHERE created_at < now();

ALTER TABLE xmp_subscriptions ADD COLUMN attempts_count INT NOT NULL DEFAULT 0;

ALTER TABLE xmp_operators ADD COLUMN rps INT NOT NULL DEFAULT 0;
UPDATE xmp_operators SET rps = 10 WHERE "name" = 'Mobilink';


CREATE TYPE transaction_statuses AS ENUM (
    '', 'failed', 'paid', 'retry_failed', 'retry_paid', 'blacklisted', 'recurly', 'rejected', 'past');

CREATE TABLE xmp_transactions (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    msisdn VARCHAR(32) NOT NULL,
    status transaction_statuses NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL,
    operator_token VARCHAR(511) NOT NULL,
    price int NOT NULL
);

CREATE TABLE xmp_retries (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    last_pay_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
    attempts_count INT NOT NULL DEFAULT 1,
    keep_days INT NOT NULL,
    delay_hours INT NOT NULL,
    msisdn VARCHAR(32) NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL
);