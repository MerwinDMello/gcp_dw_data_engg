BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSCATION;

MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_email_to_hr_status AS tgt USING (
SELECT
CASE
WHEN tgt_0.email_sent_status_id IS NOT NULL THEN tgt_0.email_sent_status_id
ELSE (
SELECT
coalesce(max(email_sent_status_id), CAST(0 as BIGNUMERIC))
FROM
{{ params.param_hr_base_views_name }}.ref_email_to_hr_status
) + CAST(row_number() OVER (ORDER BY email_sent_status_id, stg.email_sent_status_text) as BIGNUMERIC)
END AS email_sent_status_id,
stg.email_sent_status_text AS email_sent_status_text,
stg.hr_status_desc AS hr_status_desc,
stg.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
(
SELECT
trim(hrstatus) AS email_sent_status_text,
max(CASE
WHEN upper(hrstatus) = 'O' THEN 'Pre-Boarding Tour'
WHEN upper(hrstatus) = 'V' THEN 'Verification Tour'
WHEN upper(hrstatus) = 'C' THEN 'Acquisitions'
END) AS hr_status_desc,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_audit
WHERE trim(hrstatus) IS NOT NULL
GROUP BY 1, upper(CASE
WHEN upper(hrstatus) = 'O' THEN 'Pre-Boarding Tour'
WHEN upper(hrstatus) = 'V' THEN 'Verification Tour'
WHEN upper(hrstatus) = 'C' THEN 'Acquisitions'
END), 3
) AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_email_to_hr_status AS tgt_0 ON tgt_0.email_sent_status_text = stg.email_sent_status_text
AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
) AS src
ON tgt.email_sent_status_id = src.email_sent_status_id
AND upper(tgt.source_system_code) = upper(src.source_system_code)
WHEN MATCHED THEN UPDATE SET
hr_status_desc = src.hr_status_desc,
dw_last_update_date_time = src.dw_last_update_date_time
WHEN NOT MATCHED BY TARGET THEN
INSERT (email_sent_status_id, email_sent_status_text, hr_status_desc, source_system_code, dw_last_update_date_time)
VALUES (src.email_sent_status_id, src.email_sent_status_text, src.hr_status_desc, src.source_system_code, src.dw_last_update_date_time)
;
SET DUP_COUNT = (
select count(*)
from (
select
email_sent_status_id
from {{params.param_hr_core_dataset_name}}.ref_email_to_hr_status
 group by email_sent_status_id
having count(*) > 1
)
);
IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = concat('Duplicates are not allowed in the table');
ELSE
COMMIT TRANSACTION;
END IF;
END;

;
