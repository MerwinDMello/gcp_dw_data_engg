BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

UPDATE __DEFAULT_DATABASE__.`$ncr_tgt_schema`.candidate_onboarding_event AS tgt SET valid_to_date = DATE(current_ts - INTERVAL 1 DAY), dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) FROM __DEFAULT_DATABASE__.`$ncr_stg_schema`.candidate_onboarding_event_wrk AS wrk WHERE wrk.candidate_onboarding_event_sid = tgt.candidate_onboarding_event_sid
AND (coalesce(trim(wrk.event_type_id), 'XXX') <> coalesce(trim(tgt.event_type_id), 'XXX')
OR coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') <> coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
OR upper(coalesce(wrk.completed_date, '9999-12-30')) <> upper(coalesce(tgt.completed_date, '9999-12-30'))
OR coalesce(trim(wrk.candidate_sid), 'XXX') <> coalesce(trim(tgt.candidate_sid), 'XXX')
OR coalesce(trim(wrk.resource_screening_package_num), 'XXX') <> coalesce(trim(tgt.resource_screening_package_num), 'XXX')
OR coalesce(trim(wrk.sequence_num), 'XXX') <> coalesce(trim(tgt.sequence_num), 'XXX')
OR coalesce(trim(wrk.source_system_code), 'XXX') <> coalesce(trim(tgt.source_system_code), 'XXX'))
AND upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");


INSERT INTO __DEFAULT_DATABASE__.`$ncr_tgt_schema`.candidate_onboarding_event (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, valid_to_date, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
SELECT
wrk.candidate_onboarding_event_sid,
wrk.valid_from_date AS valid_from_date,
wrk.event_type_id,
wrk.recruitment_requisition_num_text,
DATETIME("9999-12-31 23:59:59") AS valid_to_date,
wrk.completed_date,
wrk.candidate_sid,
wrk.resource_screening_package_num,
wrk.sequence_num,
wrk.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
__DEFAULT_DATABASE__.`$ncr_stg_schema`.candidate_onboarding_event_wrk AS wrk
WHERE (coalesce(trim(wrk.candidate_onboarding_event_sid), CAST(123456 as STRING)), coalesce(trim(wrk.event_type_id), 'XXX'), coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX'), upper(coalesce(wrk.completed_date, '9999-12-30')), coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)), coalesce(trim(wrk.resource_screening_package_num), CAST(123456 as STRING)), coalesce(trim(wrk.sequence_num), CAST(123456 as STRING)), coalesce(trim(wrk.source_system_code), 'XXX')) NOT IN(
SELECT AS STRUCT
coalesce(trim(tgt.candidate_onboarding_event_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.event_type_id), 'XXX'),
coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX'),
upper(coalesce(tgt.completed_date, '9999-12-30')),
coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.resource_screening_package_num), CAST(123456 as STRING)),
coalesce(trim(tgt.sequence_num), CAST(123456 as STRING)),
coalesce(trim(tgt.source_system_code), 'XXX')
FROM
{{ params.param_hr_base_views_name }}.candidate_onboarding_event AS tgt
WHERE upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
)
QUALIFY row_number() OVER (PARTITION BY wrk.candidate_onboarding_event_sid, valid_from_date ORDER BY wrk.candidate_onboarding_event_sid, valid_from_date DESC) = 1
;

END;

;
