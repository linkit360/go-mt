ALTER TABLE xmp_operators ADD COLUMN mt_connection_settings jsonb NOT NULL DEFAULT '{}';
UPDATE xmp_operators SET  mt_connection_settings = '{}' WHERE "name" = 'Mobilink';

ALTER TABLE xmp_services ADD COLUMN paid_hours INT NOT NULL DEFAULT 0;
UPDATE xmp_services SET paid_hours = 24 WHERE id = 777;

ALTER TABLE xmp_subscriptions ADD COLUMN paid transaction_statuses NOT NULL DEFAULT '';
UPDATE xmp_subscriptions SET transaction_statuses = 'past' WHERE created_at < now();

CREATE TYPE transaction_statuses AS ENUM ('', 'failed', 'paid', 'blacklisted', 'recurly', 'rejected', 'past');

CREATE TABLE xmp_transactions (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    msisdn VARCHAR(32) NOT NULL,
    status transaction_statuses NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL
);

CREATE TABLE xmp_retries (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    msisdn VARCHAR(32) NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL
);