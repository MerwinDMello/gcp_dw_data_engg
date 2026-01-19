
##########################
## Variable Declaration ##
##########################

BEGIN
DECLARE
tolerance_percent,difference,srctableid,src_rec_count,tgt_rec_count int64;
declare
sourcesysnm,srctablename,tgttablename,audit_type,tableload_run_time,job_name,audit_status string;
declare
tableload_start_time,tableload_end_time,audit_time,current_ts datetime;
SET
current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET 
srctableid = Null;
SET
sourcesysnm = @p_source;
SET
srctablename = Null;
SET
tgttablename = 'edwhr.ref_submission_event_category';
SET
audit_type ='VALIDATION_COUNT';
SET
tableload_start_time = @p_tableload_start_time;
SET
tableload_end_time = @p_tableload_end_time;
SET
tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET
job_name = @p_job_name;
SET
audit_time = current_ts;
SET
tolerance_percent = 0;
SET
src_rec_count = 
(SELECT count(*)
FROM (
SELECT
    number AS submission_event_category_id,
    code,
    description,
    source_system_code,
    dw_last_update_date_time
FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_candidateselectioneventcatg

UNION ALL
SELECT
CASE
    WHEN submission_event_category_id IS NOT NULL THEN submission_event_category_id
    ELSE (
    SELECT
        coalesce(max(submission_event_category_id), CAST(1000 as BIGNUMERIC))
        FROM
        {{ params.param_hr_core_dataset_name }}.ref_submission_event_category
        for system_time as of timestamp(tableload_start_time,'US/Central') 
        WHERE upper(stg.source_system_code) = 'B'
    ) + CAST(row_number() OVER (ORDER BY submission_event_category_id, stg.code) as BIGNUMERIC)
END AS submission_event_category_id,
stg.code,
stg.description,
stg.source_system_code AS source_system_code,
stg.dw_last_update_date_time AS dw_last_update_date_time
FROM
(
SELECT DISTINCT
    actionlabel AS code,
    actionlabel AS description,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
    {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg
WHERE upper(businessview) = 'JOBAPPLICATION'

UNION ALL
SELECT DISTINCT
    type_state AS code,
    type_state AS description,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
    {{ params.param_hr_stage_dataset_name }}.ats_edw_useraction_stg
WHERE actionname NOT IN (
    SELECT DISTINCT
        businessaction
    FROM
        {{ params.param_hr_stage_dataset_name }}.ats_businessaction_bct_stg
    WHERE upper(businessview) = 'JOBAPPLICATION'
)

UNION ALL
SELECT DISTINCT
    --  Adding new action from ats_cust_v2_jobapplicationworkflowstephistory_stg
    stg_0.action AS code,
    stg_0.action AS description,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
    FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_jobapplicationworkflowstephistory_stg AS stg_0
    WHERE stg_0.action NOT IN(
    SELECT DISTINCT
        submission_event_category_code
        FROM
        {{ params.param_hr_core_dataset_name }}.ref_submission_event_category
        for system_time as of timestamp(tableload_start_time,'US/Central') 
        WHERE Upper(Trim(source_system_code)) = 'B'
    )
    AND coalesce(stg_0.action,'')<>''
    AND stg_0.triggeredby NOT LIKE 'Oferta%'
    AND stg_0.triggeredby NOT LIKE 'Offre acceptÃƒÂ©e'
    AND stg_0.triggeredby NOT LIKE 'Retirar'
    AND stg_0.triggeredby NOT LIKE 'Ã¥%'
    AND stg_0.triggeredby NOT LIKE 'Ã%'
) AS stg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_submission_event_category AS tgt_0 
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON COALESCE(UPPER(TRIM(tgt_0.submission_event_category_code)),'') = COALESCE(UPPER(TRIM(stg.code)),'')
AND COALESCE(UPPER(TRIM(tgt_0.source_system_code)),'') = COALESCE(UPPER(TRIM(stg.source_system_code)),'')));

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.ref_submission_event_category
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE);

SET
difference = CASE 
            WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
            WHEN src_rec_count =0 and tgt_rec_count = 0 Then 0
            ELSE tgt_rec_count
            END;

SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
{{ params.param_hr_audit_dataset_name }}.audit_control
VALUES
(GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type, src_rec_count, tgt_rec_count, 
cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time,
job_name, audit_time, audit_status );
END; 
