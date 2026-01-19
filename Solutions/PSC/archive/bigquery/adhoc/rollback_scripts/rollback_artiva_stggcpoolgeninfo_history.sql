DROP TABLE IF EXISTS `hca-hin-prod-cur-parallon.prod_support.artiva_stggcpoolgeninfo_history_20251222`;

CREATE TABLE `hca-hin-prod-cur-parallon.prod_support.artiva_stggcpoolgeninfo_history_20251222`
AS
SELECT * FROM `hca-hin-prod-cur-parallon.edwpsc.artiva_stggcpoolgeninfo_history`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);

ALTER TABLE `hca-hin-prod-cur-parallon.prod_support.artiva_stggcpoolgeninfo_history_20251222`
SET OPTIONS (expiration_timestamp = TIMESTAMP '2026-01-08 23:59:59');

TRUNCATE TABLE `hca-hin-prod-cur-parallon.edwpsc.artiva_stggcpoolgeninfo_history`;

INSERT INTO edwpsc.artiva_stggcpoolgeninfo_history
SELECT *
FROM `prod_support.artiva_stggcpoolgeninfo_history_20251222`
WHERE snapshot_date BETWEEN '2010-01-01T00:00:00' AND '2020-12-31T23:59:59';

INSERT INTO edwpsc.artiva_stggcpoolgeninfo_history
SELECT *
FROM `prod_support.artiva_stggcpoolgeninfo_history_20251222`
WHERE snapshot_date > '2020-12-31T23:59:59';