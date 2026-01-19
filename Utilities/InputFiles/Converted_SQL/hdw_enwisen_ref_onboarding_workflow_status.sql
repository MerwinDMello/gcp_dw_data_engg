BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSCATION;

MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow_status AS tgt USING (
SELECT
CASE
WHEN tgt_0.workflow_status_id IS NOT NULL THEN tgt_0.workflow_status_id
ELSE (
SELECT
coalesce(max(workflow_status_id), CAST(0 as BIGNUMERIC))
FROM
{{ params.param_hr_base_views_name }}.ref_onboarding_workflow_status
) + CAST(row_number() OVER (ORDER BY workflow_status_id, stg.workflow_status_text) as BIGNUMERIC)
END AS workflow_status_id,
stg.workflow_status_text AS workflow_status_text,
stg.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
(
SELECT
trim(workflowstatus) AS workflow_status_text,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_audit
WHERE trim(workflowstatus) IS NOT NULL
GROUP BY 1, 2
) AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_workflow_status AS tgt_0 ON tgt_0.workflow_status_text = stg.workflow_status_text
AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
) AS src
ON tgt.workflow_status_id = src.workflow_status_id
AND upper(tgt.source_system_code) = upper(src.source_system_code)
WHEN MATCHED THEN UPDATE SET
dw_last_update_date_time = src.dw_last_update_date_time
WHEN NOT MATCHED BY TARGET THEN
INSERT (workflow_status_id, workflow_status_text, source_system_code, dw_last_update_date_time)
VALUES (src.workflow_status_id, src.workflow_status_text, src.source_system_code, src.dw_last_update_date_time)
;
SET DUP_COUNT = (
select count(*)
from (
select
workflow_status_id
from {{params.param_hr_core_dataset_name}}.ref_onboarding_workflow_status
 group by workflow_status_id
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
