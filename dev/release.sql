ALTER TABLE xmp_operators ADD COLUMN mt_connection_settings json NOT NULL DEFAULT '{}';
UPDATE xmp_operators SET  mt_connection_settings = '{}' WHERE "name" = 'Mobilink';


ALTER TABLE xmp_services ADD COLUMN paid_hours INT NOT NULL DEFAULT 0;
UPDATE xmp_services SET paid_hours = 24 WHERE id = 777;