-- operators
ALTER TABLE xmp_operators ADD COLUMN settings jsonb NOT NULL DEFAULT '{}';
UPDATE xmp_operators SET  settings = '{}' WHERE "name" = 'Mobilink';
ALTER TABLE xmp_operators ADD COLUMN rps INT NOT NULL DEFAULT 0;
UPDATE xmp_operators SET rps = 10 WHERE "name" = 'Mobilink';

-- service new functionality
ALTER TABLE xmp_services ADD COLUMN paid_hours INT NOT NULL DEFAULT 1.0;
UPDATE xmp_services SET paid_hours = 24 WHERE id = 777;

ALTER TABLE xmp_services ADD COLUMN delay_hours INT NOT NULL DEFAULT 10;
UPDATE xmp_services SET delay_hours = 1 WHERE id = 777;

ALTER TABLE xmp_subscriptions ADD COLUMN pixel VARCHAR(511) NOT NULL DEFAULT '';
ALTER TABLE xmp_subscriptions ADD COLUMN publisher VARCHAR(511)NOT NULL DEFAULT '';
ALTER TABLE xmp_subscriptions ADD COLUMN pixel_sent boolean NOT NULL DEFAULT false;
ALTER TABLE xmp_subscriptions ADD COLUMN pixel_sent_at TIMESTAMP WITHOUT TIME ZONE;

CREATE TABLE public.xmp_subscriptions (
    id SERIAL,
    last_success_date TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    id_service INTEGER NOT NULL DEFAULT 0,
    country_code INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    msisdn CHARACTER VARYING(32),
    operator_code INTEGER NOT NULL DEFAULT 0,
    id_campaign INTEGER NOT NULL DEFAULT 0,
    attempts_count INTEGER NOT NULL DEFAULT 0,
    price INTEGER NOT NULL,
    result SUBSCRIPTION_STATUS NOT NULL DEFAULT ''::subscription_status,
    keep_days INTEGER NOT NULL DEFAULT 10,
    last_pay_attempt_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
    paid_hours INTEGER NOT NULL,
    delay_hours INTEGER NOT NULL,
    tid CHARACTER VARYING(127) NOT NULL DEFAULT ''
);
CREATE TYPE transaction_result AS ENUM ('failed', 'sms', 'paid', 'retry_failed', 'retry_paid', 'rejected', 'past');

CREATE TABLE public.xmp_transactions (
    id serial,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    msisdn CHARACTER VARYING(32),
    country_code INTEGER,
    id_service INTEGER,
    operator_code INTEGER NOT NULL DEFAULT 0,
    id_subscription INTEGER NOT NULL DEFAULT 0,
    operator_token CHARACTER VARYING(511) NOT NULL,
    price INTEGER NOT NULL,
    result TRANSACTION_RESULT NOT NULL,
    id_campaign INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE xmp_retries (
    id serial,
    tid CHARACTER VARYING(127) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    last_pay_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
    attempts_count INT NOT NULL DEFAULT 1,
    keep_days INT NOT NULL DEFAULT 0,
    delay_hours INT NOT NULL DEFAULT 0,
    msisdn VARCHAR(32) NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL
);