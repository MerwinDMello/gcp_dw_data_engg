##########################
## Variable Declaration - ref_onboarding_event_type##
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
tgttablename = 'edwhr.ref_onboarding_event_type';
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
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
from 
(
    SELECT
        DISTINCT 'D' AS event_type_code,
        'Drug Screen Completion' AS event_type_desc,
        'W' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg
      UNION DISTINCT
      SELECT
        DISTINCT 'T' AS event_type_code,
        'Tour Completion' AS event_type_desc,
        'W' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS enwisen_cm5tickets_stg_0
      UNION DISTINCT
      SELECT
        DISTINCT 'B' AS event_type_code,
        'Authorized Background Check' AS event_type_desc,
        'W' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.enwisen_cm5tickets_stg AS enwisen_cm5tickets_stg_1 ) AS stg
        );


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_event_type
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

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



##########################
## Variable Declaration - candidate_onboarding_resource##
##########################


SET
tgttablename = 'edwhr.candidate_onboarding_resource';
SET
tolerance_percent = 0;
SET
src_rec_count = 
(select count(*)
from
(SELECT
        stg.resource_screening_package_num,
        current_ts,
        stg.candidate_sid,
        stg.recruitment_requisition_sid,
        stg.valid_to_date,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.candidate_onboarding_resource AS tgt for system_time as of timestamp(tableload_start_time,'US/Central') 
        ON tgt.resource_screening_package_num = stg.resource_screening_package_num
         AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.resource_screening_package_num IS NULL));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_resource
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

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
 




##########################
## Variable Declaration - candidate_onboarding_event ##
##########################

SET
tgttablename = 'edwhr.candidate_onboarding_event';
SET
tolerance_percent = 0;
SET
src_rec_count = 
(SELECT count(*) 
from(
    SELECT
  wrk.candidate_onboarding_event_sid,
  current_ts AS valid_from_date,
  wrk.event_type_id,
  wrk.recruitment_requisition_num_text,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.completed_date,
  wrk.candidate_sid,
  wrk.resource_screening_package_num,
  wrk.sequence_num,
  wrk.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk AS wrk
WHERE
  (COALESCE(wrk.candidate_onboarding_event_sid, 123456),
    COALESCE(TRIM(wrk.event_type_id), 'XXX'),
    COALESCE(TRIM(wrk.recruitment_requisition_num_text), 'XXX'),
    COALESCE(wrk.completed_date, '9999-12-30'),
    COALESCE(wrk.candidate_sid, 123456),
    COALESCE(wrk.resource_screening_package_num, 123456),
    COALESCE(wrk.sequence_num, 123456),
    COALESCE(TRIM(wrk.source_system_code), 'XXX')) NOT IN(
  SELECT
    AS STRUCT COALESCE(CAST(tgt.candidate_onboarding_event_sid AS INT64), 123456),
    COALESCE(TRIM(tgt.event_type_id), 'XXX'),
    COALESCE(TRIM(tgt.recruitment_requisition_num_text), 'XXX'),
    COALESCE(tgt.completed_date, '9999-12-30'),
    COALESCE(tgt.candidate_sid, 123456),
    COALESCE(tgt.resource_screening_package_num, 123456),
    COALESCE(tgt.sequence_num, 123456),
    COALESCE(TRIM(tgt.source_system_code), 'XXX')
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event AS tgt for system_time as of timestamp(tableload_start_time,'US/Central') 
  WHERE
    tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") ) QUALIFY ROW_NUMBER() OVER (PARTITION BY wrk.candidate_onboarding_event_sid, valid_from_date ORDER BY wrk.candidate_onboarding_event_sid, valid_from_date DESC) = 1
)
);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

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




