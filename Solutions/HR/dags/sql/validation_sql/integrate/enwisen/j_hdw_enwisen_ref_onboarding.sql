##########################
## Variable Declaration -ref_email_to_hr_status ##
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
tgttablename = 'edwhr.ref_email_to_hr_status';
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
( SELECT
    trim(hrstatus) AS email_sent_status_text,
              CASE
                WHEN upper(hrstatus) = 'O' THEN 'Pre-Boarding Tour'
                WHEN upper(hrstatus) = 'V' THEN 'Verification Tour'
                WHEN upper(hrstatus) = 'C' THEN 'Acquisitions'
              END AS hr_status_desc,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(hrstatus) IS NOT NULL
            GROUP BY 1, 2, 3
        ) AS stg
);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_email_to_hr_status
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
## Variable Declaration - ref_onboarding_tour_status##
##########################

SET
tgttablename = 'edwhr.ref_onboarding_tour_status';
SET
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
from(
SELECT
              trim(tourstatus) AS tour_status_text,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(tourstatus) IS NOT NULL
            GROUP BY 1, 2
          UNION ALL
          SELECT
              trim(status_state) AS tour_status_text,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_resourcetransition_stg
            WHERE trim(status_state) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
        );


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour_status
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
## Variable Declaration - ref_onboarding_tour##
##########################

SET
tgttablename = 'edwhr.ref_onboarding_tour';
SET
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
from 
( SELECT
              trim(tourname) AS tour_name,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(tourname) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour
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
## Variable Declaration - ref_onboarding_workflow##
##########################

SET
tgttablename = 'edwhr.ref_onboarding_workflow';
SET
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
from 
( SELECT
              trim(workflowname) AS workflow_name,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(workflowname) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow
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
## Variable Declaration - ref_onboarding_workflow_status ##
##########################

SET
tgttablename = 'edwhr.ref_onboarding_workflow_status';
SET
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
from 
( SELECT
              trim(workflowstatus) AS workflow_status_text,
              'W' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.enwisen_audit
            WHERE trim(workflowstatus) IS NOT NULL
            GROUP BY 1, 2
        ) AS stg
);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow_status
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










