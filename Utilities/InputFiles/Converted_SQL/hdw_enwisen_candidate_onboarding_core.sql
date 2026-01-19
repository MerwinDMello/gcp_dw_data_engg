BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

UPDATE __DEFAULT_DATABASE__.`$ncr_tgt_schema`.candidate_onboarding AS tgt SET valid_to_date = DATE(current_ts - INTERVAL 1 DAY), dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) FROM __DEFAULT_DATABASE__.`$ncr_stg_schema`.candidate_onboarding_wrk AS wrk WHERE wrk.candidate_onboarding_sid = tgt.candidate_onboarding_sid
AND (coalesce(trim(wrk.requisition_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.requisition_sid), CAST(123456 as STRING))
OR coalesce(trim(wrk.employee_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.employee_sid), CAST(123456 as STRING))
OR coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING))
OR coalesce(trim(wrk.candidate_first_name), 'XXX') <> coalesce(trim(tgt.candidate_first_name), 'XXX')
OR coalesce(trim(wrk.candidate_last_name), 'XXX') <> coalesce(trim(tgt.candidate_last_name), 'XXX')
OR coalesce(trim(wrk.tour_start_date), '9999-12-30') <> coalesce(trim(tgt.tour_start_date), '9999-12-30')
OR coalesce(trim(wrk.tour_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_id), CAST(123456 as STRING))
OR coalesce(trim(wrk.tour_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_status_id), CAST(123456 as STRING))
OR coalesce(trim(wrk.tour_completion_pct), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_completion_pct), CAST(123456 as STRING))
OR coalesce(trim(wrk.workflow_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.workflow_id), CAST(123456 as STRING))
OR coalesce(trim(wrk.workflow_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.workflow_status_id), CAST(123456 as STRING))
OR coalesce(trim(wrk.email_sent_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.email_sent_status_id), CAST(123456 as STRING))
OR coalesce(trim(wrk.onboarding_confirmation_date), '9999-12-30') <> coalesce(trim(tgt.onboarding_confirmation_date), '9999-12-30')
OR coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') <> coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
OR coalesce(trim(wrk.process_level_code), 'XXX') <> coalesce(trim(tgt.process_level_code), 'XXX')
OR coalesce(trim(wrk.applicant_num), CAST(123456 as STRING)) <> coalesce(trim(tgt.applicant_num), CAST(123456 as STRING))
OR coalesce(trim(wrk.source_system_code), 'XXX') <> coalesce(trim(tgt.source_system_code), 'XXX'))
AND upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59");


INSERT INTO __DEFAULT_DATABASE__.`$ncr_tgt_schema`.candidate_onboarding (candidate_onboarding_sid, valid_from_date, valid_to_date, requisition_sid, employee_sid, candidate_sid, candidate_first_name, candidate_last_name, tour_start_date, tour_id, tour_status_id, tour_completion_pct, workflow_id, workflow_status_id, email_sent_status_id, onboarding_confirmation_date, recruitment_requisition_num_text, process_level_code, applicant_num, source_system_code, dw_last_update_date_time)
SELECT
wrk.candidate_onboarding_sid,
wrk.valid_from_date AS valid_from_date,
DATETIME("9999-12-31 23:59:59") AS valid_to_date,
wrk.requisition_sid,
wrk.employee_sid,
wrk.candidate_sid,
wrk.candidate_first_name,
wrk.candidate_last_name,
wrk.tour_start_date,
wrk.tour_id,
wrk.tour_status_id,
wrk.tour_completion_pct,
wrk.workflow_id,
wrk.workflow_status_id,
wrk.email_sent_status_id,
wrk.onboarding_confirmation_date,
wrk.recruitment_requisition_num_text,
wrk.process_level_code,
wrk.applicant_num,
wrk.source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
__DEFAULT_DATABASE__.`$ncr_stg_schema`.candidate_onboarding_wrk AS wrk
WHERE (coalesce(trim(wrk.candidate_onboarding_sid), CAST(123456 as STRING)), coalesce(trim(wrk.requisition_sid), CAST(123456 as STRING)), coalesce(trim(wrk.employee_sid), CAST(123456 as STRING)), coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)), coalesce(trim(wrk.candidate_first_name), 'XXX'), coalesce(trim(wrk.candidate_last_name), 'XXX'), coalesce(trim(wrk.tour_start_date), '9999-12-30'), coalesce(trim(wrk.tour_id), CAST(123456 as STRING)), coalesce(trim(wrk.tour_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.tour_completion_pct), CAST(123456 as STRING)), coalesce(trim(wrk.workflow_id), CAST(123456 as STRING)), coalesce(trim(wrk.workflow_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.email_sent_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.onboarding_confirmation_date), '9999-12-30'), coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX'), coalesce(trim(wrk.process_level_code), 'XXX'), coalesce(trim(wrk.applicant_num), CAST(123456 as STRING)), coalesce(trim(wrk.source_system_code), 'XXX')) NOT IN(
SELECT AS STRUCT
coalesce(trim(tgt.candidate_onboarding_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.requisition_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.employee_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING)),
coalesce(trim(tgt.candidate_first_name), 'XXX'),
coalesce(trim(tgt.candidate_last_name), 'XXX'),
coalesce(trim(tgt.tour_start_date), '9999-12-30'),
coalesce(trim(tgt.tour_id), CAST(123456 as STRING)),
coalesce(trim(tgt.tour_status_id), CAST(123456 as STRING)),
coalesce(trim(tgt.tour_completion_pct), CAST(123456 as STRING)),
coalesce(trim(tgt.workflow_id), CAST(123456 as STRING)),
coalesce(trim(tgt.workflow_status_id), CAST(123456 as STRING)),
coalesce(trim(tgt.email_sent_status_id), CAST(123456 as STRING)),
coalesce(trim(tgt.onboarding_confirmation_date), '9999-12-30'),
coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX'),
coalesce(trim(tgt.process_level_code), 'XXX'),
coalesce(trim(tgt.applicant_num), CAST(123456 as STRING)),
coalesce(trim(tgt.source_system_code), 'XXX')
FROM
{{ params.param_hr_base_views_name }}.candidate_onboarding AS tgt
WHERE upper(tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
)
;

END;

;
