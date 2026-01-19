BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

UPDATE {{ params.param_hr_core_dataset_name }}.candidate_onboarding_resource AS tgt SET valid_to_date = DATE(stg.valid_from_date - INTERVAL 1 DAY), dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk AS stg WHERE tgt.resource_screening_package_num = stg.resource_screening_package_num
AND (trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
OR trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
AND upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");
BEGIN TRANSCATION;


INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_onboarding_resource (resource_screening_package_num, valid_from_date, candidate_sid, recruitment_requisition_sid, valid_to_date, source_system_code, dw_last_update_date_time)
SELECT
stg.resource_screening_package_num,
stg.valid_from_date,
stg.candidate_sid,
stg.recruitment_requisition_sid,
stg.valid_to_date,
stg.source_system_code,
stg.dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.candidate_onboarding_resource AS tgt ON tgt.resource_screening_package_num = stg.resource_screening_package_num
AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
AND trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
AND upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
WHERE tgt.resource_screening_package_num IS NULL
;
SET DUP_COUNT = (
select count(*)
from (
select
valid_from_date,resource_screening_package_num
from {{params.param_hr_core_dataset_name}}.candidate_onboarding_resource
 group by valid_from_date,resource_screening_package_num
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
