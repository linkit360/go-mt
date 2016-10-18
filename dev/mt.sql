CREATE TYPE transaction_statuses AS ENUM ('failed', 'success', 'blacklisted', 'rejected');
CREATE TYPE transaction_types AS ENUM ('once', 'retry', 'recurly');

CREATE TABLE xmp_transaction (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    msisdn VARCHAR(32) NOT NULL,
    status transaction_statuses NOT NULL,
    type transaction_types NOT NULL,
    operator_code INTEGER NOT NULL,
    country_code INTEGER NOT NULL,
    id_service INTEGER NOT NULL,
    id_subscription INTEGER NOT NULL,
    id_content INTEGER NOT NULL,
    id_campaign INTEGER NOT NULL
);

CREATE TABLE xmp_transaction_retry (
    id serial,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    msisdn VARCHAR(32),
    operator_code INTEGER,
    country_code INTEGER,
    id_service INTEGER,
    id_subscription INTEGER,
    id_content INTEGER,
    id_campaign INTEGER
);