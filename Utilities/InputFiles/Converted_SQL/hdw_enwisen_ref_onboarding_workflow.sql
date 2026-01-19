BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSCATION;

MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_onboarding_workflow AS tgt USING (
SELECT
CASE
WHEN tgt_0.workflow_id IS NOT NULL THEN tgt_0.workflow_id
ELSE (
SELECT
coalesce(max(workflow_id), CAST(0 as BIGNUMERIC))
FROM
{{ params.param_hr_base_views_name }}.ref_onboarding_workflow
) + CAST(row_number() OVER (ORDER BY workflow_id, stg.workflow_name) as BIGNUMERIC)
END AS workflow_id,
stg.workflow_name AS workflow_name,
stg.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
(
SELECT
trim(workflowname) AS workflow_name,
'W' AS source_system_code
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_audit
WHERE trim(workflowname) IS NOT NULL
GROUP BY 1, 2
) AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_workflow AS tgt_0 ON tgt_0.workflow_name = stg.workflow_name
AND upper(tgt_0.source_system_code) = upper(stg.source_system_code)
) AS src
ON tgt.workflow_id = src.workflow_id
AND upper(tgt.source_system_code) = upper(src.source_system_code)
WHEN MATCHED THEN UPDATE SET
dw_last_update_date_time = src.dw_last_update_date_time
WHEN NOT MATCHED BY TARGET THEN
INSERT (workflow_id, workflow_name, source_system_code, dw_last_update_date_time)
VALUES (src.workflow_id, src.workflow_name, src.source_system_code, src.dw_last_update_date_time)
;
SET DUP_COUNT = (
select count(*)
from (
select
workflow_id
from {{params.param_hr_core_dataset_name}}.ref_onboarding_workflow
 group by workflow_id
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
