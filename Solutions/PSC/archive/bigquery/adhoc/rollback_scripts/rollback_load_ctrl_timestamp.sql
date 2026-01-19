DROP TABLE IF EXISTS `hca-hin-prod-cur-parallon.prod_support.load_ctrl_timestamp_20251222`;

CREATE TABLE `hca-hin-prod-cur-parallon.prod_support.load_ctrl_timestamp_20251222`
AS
SELECT * FROM `hca-hin-prod-cur-parallon.edwpsc_ac.load_ctrl_timestamp`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

ALTER TABLE `hca-hin-prod-cur-parallon.prod_support.load_ctrl_timestamp_20251222`
SET OPTIONS (expiration_timestamp = TIMESTAMP '2026-01-08 23:59:59');

TRUNCATE TABLE `hca-hin-prod-cur-parallon.edwpsc_ac.load_ctrl_timestamp`;

INSERT INTO `hca-hin-prod-cur-parallon.edwpsc_ac.load_ctrl_timestamp`
SELECT * FROM `hca-hin-prod-cur-parallon.prod_support.load_ctrl_timestamp_20251222`;