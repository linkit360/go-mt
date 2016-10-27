-- operators
ALTER TABLE xmp_operators ADD COLUMN settings jsonb NOT NULL DEFAULT '{}';
UPDATE xmp_operators SET  settings = '{}' WHERE "name" = 'Mobilink';
ALTER TABLE xmp_operators ADD COLUMN rps INT NOT NULL DEFAULT 0;
UPDATE xmp_operators SET rps = 10 WHERE "name" = 'Mobilink';

-- service new functionality
ALTER TABLE xmp_services ADD COLUMN paid_hours INT NOT NULL DEFAULT 0;
UPDATE xmp_services SET paid_hours = 24 WHERE id = 777;

-- subscriptions table
CREATE TYPE subscription_status AS ENUM ('', 'failed', 'paid', 'blacklisted', 'rejected', 'past');
ALTER TABLE xmp_subscriptions ADD COLUMN result subscription_status NOT NULL DEFAULT '';
UPDATE xmp_subscriptions SET result = 'past' WHERE created_at < now();

ALTER TABLE xmp_subscriptions ADD COLUMN keep_days INT NOT NULL DEFAULT 10;
ALTER TABLE xmp_subscriptions ADD COLUMN attempts_count INT NOT NULL DEFAULT 0;
ALTER TABLE xmp_subscriptions ADD COLUMN last_pay_attempt_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE xmp_subscriptions ADD COLUMN price int NOT NULL;
ALTER TABLE xmp_subscriptions ADD COLUMN tid varchar(127) NOT NULL DEFAULT '';

-- transactions table
CREATE TYPE transaction_result AS ENUM ('failed', 'sms', 'paid', 'retry_failed', 'retry_paid', 'rejected', 'past');
ALTER TABLE xmp_transactions ADD COLUMN result transaction_result NOT NULL;
UPDATE xmp_transactions SET result = 'past' WHERE created_at < now();
ALTER TABLE xmp_transactions ADD COLUMN operator_token VARCHAR(511) NOT NULL;
ALTER TABLE xmp_transactions ADD COLUMN price int NOT NULL;
ALTER TABLE xmp_transactions ADD COLUMN id_campaign INT NOT NULL DEFAULT 0;

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