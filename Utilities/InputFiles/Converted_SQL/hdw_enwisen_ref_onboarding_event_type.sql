BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSCATION;

MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_event_type AS tgt USING (
SELECT
CASE
WHEN tgt_0.event_type_id IS NOT NULL THEN tgt_0.event_type_id
ELSE (
SELECT
coalesce(max(event_type_id), CAST(0 as BIGNUMERIC))
FROM
{{ params.param_hr_base_views_name }}.ref_onboarding_event_type
) + CAST(row_number() OVER (ORDER BY event_type_id, upper(stg.event_type_code)) as BIGNUMERIC)
END AS event_type_id,
stg.event_type_code AS event_type_code,
stg.event_type_desc AS event_type_desc,
stg.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
(
SELECT DISTINCT
'D' AS event_type_code,
'Drug Screen Completion' AS event_type_desc,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
UNION DISTINCT
SELECT DISTINCT
'T' AS event_type_code,
'Tour Completion' AS event_type_desc,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
UNION DISTINCT
SELECT DISTINCT
'B' AS event_type_code,
'Authorized Background Check' AS event_type_desc,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
) AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_event_type AS tgt_0 ON upper(tgt_0.event_type_code) = upper(stg.event_type_code)
) AS src
ON tgt.event_type_id = src.event_type_id
WHEN MATCHED THEN UPDATE SET
event_type_desc = src.event_type_desc,
dw_last_update_date_time = src.dw_last_update_date_time
WHEN NOT MATCHED BY TARGET THEN
INSERT (event_type_id, event_type_code, event_type_desc, source_system_code, dw_last_update_date_time)
VALUES (src.event_type_id, src.event_type_code, src.event_type_desc, src.source_system_code, src.dw_last_update_date_time)
;
SET DUP_COUNT = (
select count(*)
from (
select
event_type_id
from {{params.param_hr_core_dataset_name}}.ref_onboarding_event_type
 group by event_type_id
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
