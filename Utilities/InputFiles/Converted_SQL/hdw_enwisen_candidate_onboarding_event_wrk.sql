BEGIN
DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_wrk;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_wrk (requisition_number, process_level_code, data_type, dw_last_update_date_time)
SELECT
iq.requisition_number,
iq.process_level_code,
iq.data_type,
iq.dw_last_update_date_time
FROM
(
SELECT DISTINCT
trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), 1, strpos(reverse(enwisen_cm5tickets_stg.subject), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
'T' AS data_type,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
WHERE upper(enwisen_cm5tickets_stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(enwisen_cm5tickets_stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(enwisen_cm5tickets_stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
AND strpos(enwisen_cm5tickets_stg.subject, '-') <> 0
AND upper(enwisen_cm5tickets_stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
UNION DISTINCT
SELECT DISTINCT
trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), 1, strpos(reverse(enwisen_cm5tickets_stg.subject), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
'D' AS data_type,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
WHERE upper(enwisen_cm5tickets_stg.topic) = 'HIRE MY EMPLOYEES'
AND upper(enwisen_cm5tickets_stg.category) = 'HIRING EMPLOYEES'
AND upper(enwisen_cm5tickets_stg.subcategory) = 'DRUG SCREEN RESULTS'
AND upper(enwisen_cm5tickets_stg.subject) LIKE 'CONFIRMED DS RESULTS%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
UNION DISTINCT
SELECT DISTINCT
trim(reverse(trim(substr(replace(reverse(enwisen_cm5tickets_stg.subject), ')', ''), 1, strpos(replace(reverse(enwisen_cm5tickets_stg.subject), ')', ''), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
'B' AS data_type,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
WHERE upper(enwisen_cm5tickets_stg.topic) = 'Z-AUTO-BG CHECK AUTH'
AND upper(enwisen_cm5tickets_stg.category) = 'Z-AUTO-BG CHECK AUTH'
AND upper(enwisen_cm5tickets_stg.subcategory) = 'Z-AUTO-BG CHECK AUTH'
AND upper(enwisen_cm5tickets_stg.subject) LIKE 'BG AUTH COMPLETE%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
AND strpos(enwisen_cm5tickets_stg.subject, '-') <> 0
) AS iq
WHERE length(iq.requisition_number) <> 0
AND length(iq.process_level_code) <> 0
AND lower(iq.requisition_number) = upper(iq.requisition_number)
AND lower(iq.process_level_code) = upper(iq.process_level_code)
AND iq.requisition_number NOT LIKE '%*%'
AND iq.requisition_number NOT LIKE '%/%'
AND iq.requisition_number NOT LIKE '%)%'
AND iq.requisition_number NOT LIKE '%(%'
AND iq.requisition_number NOT LIKE '%.%'
AND iq.requisition_number NOT LIKE '%]%'
AND iq.requisition_number NOT LIKE '%[%'
AND iq.requisition_number NOT LIKE '% %'
AND iq.requisition_number NOT LIKE '%:%'
AND iq.requisition_number NOT LIKE '%\'%'
;


CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}", 'CANDIDATE_ONBOARDING_EVENT_XWLK_WRK', 'Coalesce(Trim(Requisition_Number)||Trim(Process_Level_Code)||Trim(Data_Type),-1)', 'CANDIDATE_ONBOARDING_EVENT');


TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
roe.event_type_id AS event_type_id,
concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1))))) AS recruitment_requisition_num_text,
CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
CAST(NULL as INT64) AS candidate_sid,
CAST(NULL as INT64) AS resource_screening_package_num,
CAST(NULL as INT64) AS sequence_num,
'W' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(reverse(subject), 1, strpos(reverse(subject), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'T'), CAST(-1 as STRING)) = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'T'
WHERE upper(topic) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(category) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
AND strpos(subject, '-') <> 0
AND upper(subject) LIKE 'ONBOARDING ACTION REQUIRED %'
AND upper(subject) NOT LIKE '%ACQ -%'
AND upper(subject) NOT LIKE '%ACQ-%'
AND upper(subject) NOT LIKE '%ACQ %'
AND upper(subject) NOT LIKE '%ACQUISITION%'
QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
UNION DISTINCT
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
roe.event_type_id AS event_type_id,
concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1))))) AS recruitment_requisition_num_text,
CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
CAST(NULL as INT64) AS candidate_sid,
CAST(NULL as INT64) AS resource_screening_package_num,
CAST(NULL as INT64) AS sequence_num,
'W' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(reverse(subject), 1, strpos(reverse(subject), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'D'), CAST(-1 as STRING)) = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'D'
WHERE upper(topic) = 'HIRE MY EMPLOYEES'
AND upper(category) = 'HIRING EMPLOYEES'
AND upper(subcategory) = 'DRUG SCREEN RESULTS'
AND upper(subject) LIKE 'CONFIRMED DS RESULTS%'
AND upper(subject) NOT LIKE '%ACQ -%'
AND upper(subject) NOT LIKE '%ACQ-%'
AND upper(subject) NOT LIKE '%ACQ %'
AND upper(subject) NOT LIKE '%ACQUISITION%'
QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
UNION DISTINCT
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
roe.event_type_id AS event_type_id,
concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(replace(reverse(subject), ')', ''), 1, strpos(replace(reverse(subject), ')', ''), '-') - 1))))) AS recruitment_requisition_num_text,
CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
CAST(NULL as INT64) AS candidate_sid,
CAST(NULL as INT64) AS resource_screening_package_num,
CAST(NULL as INT64) AS sequence_num,
'W' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(replace(reverse(subject), ')', ''), 1, strpos(replace(reverse(subject), ')', ''), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'B'), CAST(-1 as STRING)) = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
INNER JOIN {{ params.param_hr_base_views_name }}.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'B'
WHERE upper(topic) = 'Z-AUTO-BG CHECK AUTH'
AND upper(category) = 'Z-AUTO-BG CHECK AUTH'
AND upper(subcategory) = 'Z-AUTO-BG CHECK AUTH'
AND upper(subject) LIKE 'BG AUTH COMPLETE%'
AND upper(subject) NOT LIKE '%ACQ -%'
AND upper(subject) NOT LIKE '%ACQ-%'
AND upper(subject) NOT LIKE '%ACQ %'
AND upper(subject) NOT LIKE '%ACQUISITION%'
AND strpos(subject, '-') <> 0
QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
;


TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_ats;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_ats (jobrequisition, candidate, event_type_id, dw_last_update_date_time)
SELECT
max(trim(ats_resourcetransition_bct_stg.jobrequisition)) AS jobrequisition,
max(trim(ats_resourcetransition_bct_stg.candidate)) AS candidate,
'2' AS event_type_id,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.ats_resourcetransition_bct_stg
WHERE upper(ats_resourcetransition_bct_stg.status_state) = 'COMPLETE'
GROUP BY upper(trim(ats_resourcetransition_bct_stg.jobrequisition)), upper(trim(ats_resourcetransition_bct_stg.candidate)), 4
UNION DISTINCT
SELECT
max(trim(ats_resourcescreening_bct_stg.resourcescreeningpackage)) AS jobrequisition,
max(trim(ats_resourcescreening_bct_stg.sequencenumber)) AS candidate,
'1' AS event_type_id,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.ats_resourcescreening_bct_stg
WHERE upper(ats_resourcescreening_bct_stg.hcascrevendstat) = 'COMPLETED'
AND upper(ats_resourcescreening_bct_stg.screening) = 'DRUGSCREEN'
GROUP BY upper(trim(ats_resourcescreening_bct_stg.resourcescreeningpackage)), upper(trim(ats_resourcescreening_bct_stg.sequencenumber)), 4
;


CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}", 'CANDIDATE_ONBOARDING_EVENT_XWLK_ATS', 'TRIM(JOBREQUISITION)||TRIM(CANDIDATE)||EVENT_TYPE_ID||\'-B\'', 'CANDIDATE_ONBOARDING_EVENT');


TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg (requisition_num, candidate_num, event_type_id, recruitment_requisition_num_text, creation_date_time, candidate_sid, dw_last_update_date_time)
SELECT
max(trim(s.requisition_num)) AS requisition_num,
max(trim(s.candidate_num)) AS candidate_num,
'3' AS event_type_id,
rr.recruitment_requisition_num_text,
st.creation_date_time,
s.candidate_sid,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_base_views_name }}.submission_tracking AS st
INNER JOIN {{ params.param_hr_base_views_name }}.submission_tracking_status AS sts ON st.submission_tracking_sid = sts.submission_tracking_sid
AND upper(sts.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(sts.source_system_code) = 'B'
INNER JOIN {{ params.param_hr_base_views_name }}.ref_submission_status AS rss ON sts.submission_status_id = rss.submission_status_id
AND upper(rss.source_system_code) = 'B'
INNER JOIN {{ params.param_hr_base_views_name }}.candidate_profile AS cp ON st.candidate_profile_sid = cp.candidate_profile_sid
AND upper(cp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(cp.source_system_code) = 'B'
INNER JOIN {{ params.param_hr_base_views_name }}.submission AS s ON cp.candidate_profile_sid = s.candidate_profile_sid
AND upper(s.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(s.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.recruitment_requisition AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
AND upper(rr.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(rr.source_system_code) = 'B'
WHERE upper(st.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(st.source_system_code) = 'B'
AND upper(rss.submission_status_desc) = 'CANDIDATE CONSENT PENDING'
GROUP BY upper(trim(s.requisition_num)), upper(trim(s.candidate_num)), 4, 5, 6, 7
;


CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen("{{ params.param_hr_stage_dataset_name }}", 'CANDIDATE_ONBOARDING_EVENT_XWLK_STG', 'REQUISITION_NUM||CANDIDATE_NUM||EVENT_TYPE_ID||\'-B\'', 'CANDIDATE_ONBOARDING_EVENT');


INSERT INTO {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
'2' AS event_type_id,
req.recruitment_requisition_num_text AS recruitment_requisition_num_text,
stg.completion_date AS completed_date,
can.candidate_sid AS candidate_sid,
orc.resource_screening_package_num AS resource_screening_package_num,
CAST(NULL as INT64) AS sequence_num,
'B' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.ats_resourcetransition_bct_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.jobrequisition), trim(stg.candidate), '2', '-B') = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.recruitment_requisition AS req ON req.requisition_num = stg.jobrequisition
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(req.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.candidate AS can ON can.candidate_num = stg.candidate
AND upper(can.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(can.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.candidate_onboarding_resource AS orc ON orc.candidate_sid = can.candidate_sid
AND upper(orc.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(orc.source_system_code) = 'B'
WHERE upper(stg.status_state) = 'COMPLETE'
QUALIFY row_number() OVER (PARTITION BY stg.jobrequisition, stg.candidate ORDER BY stg.updatestamp DESC) = 1
UNION DISTINCT
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
'1' AS event_type_id,
req.recruitment_requisition_num_text AS recruitment_requisition_num_text,
stg.hcascrevendstatdate AS completed_date,
orc.candidate_sid AS candidate_sid,
stg.resourcescreeningpackage AS resource_screening_package_num,
stg.sequencenumber AS sequence_num,
'B' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.ats_resourcescreening_bct_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat(trim(stg.resourcescreeningpackage), trim(stg.sequencenumber), '1', '-B')) = upper(xwlk.sk_source_txt)
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.candidate_onboarding_resource AS orc ON orc.resource_screening_package_num = stg.resourcescreeningpackage
AND upper(orc.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(orc.source_system_code) = 'B'
LEFT OUTER JOIN {{ params.param_hr_base_views_name }}.recruitment_requisition AS req ON req.recruitment_requisition_sid = orc.recruitment_requisition_sid
AND upper(req.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(req.source_system_code) = 'B'
WHERE upper(stg.hcascrevendstat) = 'COMPLETED'
AND upper(stg.screening) = 'DRUGSCREEN'
QUALIFY row_number() OVER (PARTITION BY resource_screening_package_num, stg.sequencenumber ORDER BY stg.updatestamp DESC) = 1
UNION DISTINCT
SELECT
xwlk.sk AS candidate_onboarding_event_sid,
current_ts AS valid_from_date,
stg.event_type_id AS event_type_id,
stg.recruitment_requisition_num_text AS recruitment_requisition_num_text,
stg.creation_date_time AS completed_date,
stg.candidate_sid AS candidate_sid,
NULL AS resource_screening_package_num,
CAST(NULL as INT64) AS sequence_num,
'B' AS source_system_code,
timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_stg AS stg
INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(stg.requisition_num, stg.candidate_num, stg.event_type_id, '-B') = xwlk.sk_source_txt
AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
QUALIFY row_number() OVER (PARTITION BY stg.requisition_num, stg.candidate_num ORDER BY stg.creation_date_time DESC) = 1
;


INSERT INTO {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg_reject (ticketcode, userid, archived, casetype, ticketstatus, createddatetime, sladate, closeddatetime, lasteditdatetime, resolveddatetime, topic, category, subcategory, source_code, servicegroup, substatus, isfirstcallresolution, creatoruserid, closeuserid, lastedituserid, owneruserid, chatagentuserid, regardinguserid, creatorfirstname, creatorlastname, creatorname, contactmethod, contactname, contactrelationshipname, aboutee, email, firstname, lastname, surveydatetime, surveyid, surveyanswer1, surveyanswer2, surveyanswer3, surveyanswer4, surveyanswer5, surveyscore, surveyagreementresponse, population, priority, processtime, reminderdatetime, reminderemail, reminderphone, secure, showtoee, customcheckbox1, customcheckbox2, customcheckbox3, customcheckbox4, customcheckbox5, customcheckbox6, customdate1, customdate2, customdate3, customdate4, customdate5, customdate6, customselect1, customselect2, customselect3, customselect4, customselect5, customselect6, customstring1, customstring2, customstring3, customstring4, customstring5, customstring6, surveyfollowup, knowledgedomain, remindernote, surveycommentresponse, subject, resolution, issue, dw_last_update_date_time)
SELECT DISTINCT
iq.ticketcode,
iq.userid,
iq.archived,
iq.casetype,
iq.ticketstatus,
iq.createddatetime,
iq.sladate,
iq.closeddatetime,
iq.lasteditdatetime,
iq.resolveddatetime,
iq.topic,
iq.category,
iq.subcategory,
iq.source_code,
iq.servicegroup,
iq.substatus,
iq.isfirstcallresolution,
iq.creatoruserid,
iq.closeuserid,
iq.lastedituserid,
iq.owneruserid,
iq.chatagentuserid,
iq.regardinguserid,
iq.creatorfirstname,
iq.creatorlastname,
iq.creatorname,
iq.contactmethod,
iq.contactname,
iq.contactrelationshipname,
iq.aboutee,
iq.email,
iq.firstname,
iq.lastname,
iq.surveydatetime,
iq.surveyid,
iq.surveyanswer1,
iq.surveyanswer2,
iq.surveyanswer3,
iq.surveyanswer4,
iq.surveyanswer5,
iq.surveyscore,
iq.surveyagreementresponse,
iq.population,
iq.priority,
iq.processtime,
iq.reminderdatetime,
iq.reminderemail,
iq.reminderphone,
iq.secure,
iq.showtoee,
iq.customcheckbox1,
iq.customcheckbox2,
iq.customcheckbox3,
iq.customcheckbox4,
iq.customcheckbox5,
iq.customcheckbox6,
iq.customdate1,
iq.customdate2,
iq.customdate3,
iq.customdate4,
iq.customdate5,
iq.customdate6,
iq.customselect1,
iq.customselect2,
iq.customselect3,
iq.customselect4,
iq.customselect5,
iq.customselect6,
iq.customstring1,
iq.customstring2,
iq.customstring3,
iq.customstring4,
iq.customstring5,
iq.customstring6,
iq.surveyfollowup,
iq.knowledgedomain,
iq.remindernote,
iq.surveycommentresponse,
iq.subject,
iq.resolution,
iq.issue,
iq.dw_last_update_date_time
FROM
(
SELECT DISTINCT
trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
stg.*
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
WHERE upper(stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
AND upper(stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
AND strpos(stg.subject, '-') <> 0
AND upper(stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
AND upper(stg.subject) NOT LIKE '%ACQ -%'
AND upper(stg.subject) NOT LIKE '%ACQ-%'
AND upper(stg.subject) NOT LIKE '%ACQ %'
AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
UNION DISTINCT
SELECT DISTINCT
trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
stg.*
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
WHERE upper(stg.topic) = 'HIRE MY EMPLOYEES'
AND upper(stg.category) = 'HIRING EMPLOYEES'
AND upper(stg.subcategory) = 'DRUG SCREEN RESULTS'
AND upper(stg.subject) LIKE 'CONFIRMED DS RESULTS%'
AND upper(stg.subject) NOT LIKE '%ACQ -%'
AND upper(stg.subject) NOT LIKE '%ACQ-%'
AND upper(stg.subject) NOT LIKE '%ACQ %'
AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
UNION DISTINCT
SELECT DISTINCT
trim(reverse(trim(substr(replace(reverse(stg.subject), ')', ''), 1, strpos(replace(reverse(stg.subject), ')', ''), '-') - 1)))) AS requisition_number,
trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
stg.*
FROM
{{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS stg
WHERE upper(stg.topic) = 'Z-AUTO-BG CHECK AUTH'
AND upper(stg.category) = 'Z-AUTO-BG CHECK AUTH'
AND upper(stg.subcategory) = 'Z-AUTO-BG CHECK AUTH'
AND upper(stg.subject) LIKE 'BG AUTH COMPLETE%'
AND upper(stg.subject) NOT LIKE '%ACQ -%'
AND upper(stg.subject) NOT LIKE '%ACQ-%'
AND upper(stg.subject) NOT LIKE '%ACQ %'
AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
AND strpos(stg.subject, '-') <> 0
) AS iq
WHERE length(iq.requisition_number) = 0
OR length(iq.process_level_code) = 0
OR lower(iq.requisition_number) <> upper(iq.requisition_number)
OR lower(iq.process_level_code) <> upper(iq.process_level_code)
OR iq.requisition_number LIKE '%*%'
OR iq.requisition_number LIKE '%/%'
OR iq.requisition_number LIKE '%)%'
OR iq.requisition_number LIKE '%(%'
OR iq.requisition_number LIKE '%.%'
OR iq.requisition_number LIKE '%]%'
OR iq.requisition_number LIKE '%[%'
OR iq.requisition_number LIKE '% %'
OR iq.requisition_number LIKE '%:%'
OR iq.requisition_number LIKE '%\'%'
;

END;

;
