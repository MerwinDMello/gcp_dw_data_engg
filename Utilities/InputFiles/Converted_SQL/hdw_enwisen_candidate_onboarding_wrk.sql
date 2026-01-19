BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);


CALL edwhr_procs.sk_gen('$NCR_STG_SCHEMA', 'Enwisen_Audit', 'Coalesce(Trim(EmployeeID)||Trim(HRCONumber)||Trim(RequisitionNumber)||Trim(ApplicantNumber),-1)', 'CANDIDATE_ONBOARDING');


CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}", 'ATS_ResourceTransition_BCT_STG', 'Coalesce(Trim(Cast(Candidate AS Varchar(15)))||Trim(JobRequisition),-1)||\'-ATS\'', 'CANDIDATE_ONBOARDING');


TRUNCATE TABLE __DEFAULT_DATABASE__.`$ncr_stg_schema`.candidate_onboarding_wrk;


CREATE TEMPORARY TABLE can
CLUSTER BY social_security_num, requisition_num, lawson_company_num
AS
SELECT DISTINCT
cp.candidate_sid,
trim(req.requisition_num) AS requisition_num,
trim(req.lawson_company_num) AS lawson_company_num,
trim(concat(substr('000000000', 1, 9 - length(replace(replace(replace(replace(replace(trim(cp.social_security_num), '-', ''), '.', ''), '/', ''), ' ', ''), '_', ''))), replace(replace(replace(replace(replace(trim(cp.social_security_num), '-', ''), '.', ''), '/', ''), ' ', ''), '_', ''))) AS social_security_num
FROM
{{ params.param_hr_base_views_name }}.candidate_person AS cp
INNER JOIN {{ params.param_hr_base_views_name }}.submission AS s ON trim(s.candidate_sid) = trim(cp.candidate_sid)
AND upper(cp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND length(replace(replace(replace(replace(replace(trim(cp.social_security_num), '-', ''), '.', ''), '/', ''), ' ', ''), '_', '')) < 10
AND upper(s.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(cp.source_system_code) = 'T'
AND upper(s.source_system_code) = 'T'
INNER JOIN {{ params.param_hr_base_views_name }}.recruitment_requisition AS rr ON trim(s.recruitment_requisition_sid) = trim(rr.recruitment_requisition_sid)
AND upper(rr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(rr.source_system_code) = 'T'
INNER JOIN {{ params.param_hr_base_views_name }}.requisition AS req ON trim(req.requisition_sid) = trim(rr.lawson_requisition_sid)
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk (candidate_onboarding_sid, valid_from_date, requisition_sid, employee_sid, candidate_sid, candidate_first_name, candidate_last_name, tour_start_date, tour_id, tour_status_id, tour_completion_pct, workflow_id, workflow_status_id, email_sent_status_id, onboarding_confirmation_date, recruitment_requisition_num_text, process_level_code, applicant_num, source_system_code, dw_last_update_date_time)
SELECT
xwlk.sk AS candidate_onboarding_sid,
current_ts AS valid_from_date,
req.requisition_sid AS requisition_sid,
emp.employee_sid AS employee_sid,
can.candidate_sid AS candidate_sid,
trim(stg.firstname) AS candidate_first_name,
trim(stg.lastname) AS candidate_last_name,
DATE(stg.basedate + INTERVAL 100 YEAR) AS tour_start_date,
trim(rot.tour_id) AS tour_id,
trim(rots.tour_status_id) AS tour_status_id,
bqutil.fn.cw_round_half_even(CASE
trim(stg.tourpercent)
WHEN '' THEN NUMERIC '0'
ELSE CAST(trim(stg.tourpercent) as NUMERIC)
END, 3) AS tour_completion_pct,
trim(row1.workflow_id) AS workflow_id,
trim(rows1.workflow_status_id) AS workflow_status_id,
trim(reh.email_sent_status_id) AS email_sent_status_id,
parse_date('%Y-%m-%d', substr(stg.approvaldate, 1, 10)) AS onboarding_confirmation_date,
concat(trim(stg.processlevel), '-', trim(stg.requisitionnumber)) AS recruitment_requisition_num_text,
trim(stg.processlevel) AS process_level_code,
CASE
trim(stg.applicantnumber)
WHEN '' THEN 0
ELSE CAST(trim(stg.applicantnumber) as INT64)
END AS applicant_num,
'W' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_audit AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(employeeid), trim(hrconumber), trim(requisitionnumber), trim(applicantnumber)), CAST(-1 as STRING)) = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING'
INNER JOIN {{ params.param_hr_base_views_name }}.requisition AS req ON trim(stg.requisitionnumber) = trim(req.requisition_num)
AND trim(stg.hrconumber) = trim(req.lawson_company_num)
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_tour AS rot ON trim(stg.tourname) = trim(rot.tour_name)
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_tour_status AS rots ON trim(stg.tourstatus) = trim(rots.tour_status_text)
AND upper(rots.source_system_code) = 'W'
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_workflow AS row1 ON trim(stg.workflowname) = trim(row1.workflow_name)
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_workflow_status AS rows1 ON trim(stg.workflowstatus) = trim(rows1.workflow_status_text)
INNER JOIN {{ params.param_hr_base_views_name }}.ref_email_to_hr_status AS reh ON trim(stg.hrstatus) = trim(reh.email_sent_status_text)
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.employee AS emp ON trim(stg.employeeid) = trim(emp.employee_num)
AND trim(stg.hrconumber) = trim(emp.lawson_company_num)
AND upper(emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
LEFT OUTER JOIN can ON trim(stg.ssn) = can.social_security_num
AND trim(stg.requisitionnumber) = can.requisition_num
AND trim(stg.hrconumber) = can.lawson_company_num
WHERE upper(stg.approvaldate) LIKE '20%'
AND upper(stg.datastatus) = 'SENT'
QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_sid ORDER BY stg.approvaldate DESC) = 1
;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk (candidate_onboarding_sid, valid_from_date, requisition_sid, employee_sid, candidate_sid, candidate_first_name, candidate_last_name, tour_start_date, tour_id, tour_status_id, tour_completion_pct, workflow_id, workflow_status_id, email_sent_status_id, onboarding_confirmation_date, recruitment_requisition_num_text, process_level_code, applicant_num, source_system_code, dw_last_update_date_time)
SELECT
xwlk.sk AS candidate_onboarding_sid,
current_ts AS valid_from_date,
req.lawson_requisition_sid AS requisition_sid,
NULL AS employee_sid,
trim(can.candidate_sid) AS candidate_sid,
trim(canp.first_name) AS candidate_first_name,
trim(canp.last_name) AS candidate_last_name,
stg.createstamp AS tour_start_date,
NULL AS tour_id,
trim(rots.tour_status_id) AS tour_status_id,
NULL AS tour_completion_pct,
NULL AS workflow_id,
NULL AS workflow_status_id,
NULL AS email_sent_status_id,

sub.onboarding_confirmation_date AS onboarding_confirmation_date,
trim(req.recruitment_requisition_num_text) AS recruitment_requisition_num_text,
trim(req.process_level_code) AS process_level_code,
NULL AS applicant_num,
'B' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.ats_resourcetransition_bct_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(coalesce(concat(trim(candidate), trim(jobrequisition)), CAST(-1 as STRING)), '-ATS') = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.recruitment_requisition AS req ON CASE
trim(stg.jobrequisition)
WHEN '' THEN 0
ELSE CAST(trim(stg.jobrequisition) as INT64)
END = CASE
trim(req.requisition_num)
WHEN '' THEN 0.0
ELSE CAST(trim(req.requisition_num) as FLOAT64)
END
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(req.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.candidate AS can ON trim(stg.candidate) = trim(can.candidate_num)
AND upper(can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(can.source_system_code) = 'B'
INNER JOIN {{ params.param_hr_base_views_name }}.candidate_person AS canp ON trim(can.candidate_sid) = trim(canp.candidate_sid)
AND upper(canp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(canp.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_tour_status AS rots ON trim(stg.status_state) = trim(rots.tour_status_text)
AND upper(rots.source_system_code) = 'B'
LEFT OUTER JOIN (
SELECT
s.candidate_profile_sid,
st.creation_date_time AS onboarding_confirmation_date,
s.recruitment_requisition_sid,
s.candidate_num
FROM
{{ params.param_hr_base_views_name }}.submission AS s
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.submission_tracking AS st ON s.candidate_profile_sid = st.candidate_profile_sid
AND upper(st.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(st.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.submission_tracking_status AS sts ON st.submission_tracking_sid = sts.submission_tracking_sid
AND upper(sts.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(sts.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.ref_submission_status AS rss ON rss.submission_status_id = sts.submission_status_id
WHERE upper(rss.submission_status_name) = 'CONFIRMED COMPLETED'
AND upper(s.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(s.source_system_code) = 'B'
QUALIFY row_number() OVER (PARTITION BY s.candidate_profile_sid ORDER BY onboarding_confirmation_date) = 1
) AS sub ON sub.recruitment_requisition_sid = req.recruitment_requisition_sid
AND sub.candidate_num = stg.candidate
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(req.source_system_code) = 'B'
QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_sid ORDER BY stg.updatestamp DESC) = 1
;

END;

;
