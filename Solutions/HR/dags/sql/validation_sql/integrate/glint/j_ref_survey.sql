
/*********************************************************************************
* Start: J_HDW_GLINT_REF_QUESTION_TYPE.SQL
**********************************************************************************/

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
tgttablename = 'edwhr.ref_question_type';
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
(SELECT COUNT(*) FROM
(SELECT
LPAD(CAST(ROW_NUMBER() OVER (ORDER BY a.question_type) + (
    SELECT
      CAST(COALESCE(MAX(question_type_code), '0') AS INT64)
    FROM
      {{ params.param_hr_core_dataset_name }}.ref_question_type
      for system_time as of timestamp(tableload_start_time,'US/Central') 
    WHERE
      UPPER(question_type_code) <> 'UNK' ) AS STRING), 10),
a.question_type,
'G' AS source_system_code,
DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM (
SELECT
  glint_question.question_type
FROM
  {{ params.param_hr_stage_dataset_name }}.glint_question
GROUP BY
  1 ) AS a
WHERE
TRIM(a.question_type) NOT IN(
SELECT
  DISTINCT TRIM(question_type_desc)
FROM
  {{ params.param_hr_core_dataset_name }}.ref_question_type AS ref_question_type_0
  for system_time as of timestamp(tableload_start_time,'US/Central') 
WHERE
  upper(Trim(Source_System_Code)) = 'G' )));

SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.ref_question_type
   WHERE upper(Trim(Source_System_Code)) = 'G'
     AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE )a);

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

/*********************************************************************************
* End: J_HDW_GLINT_REF_QUESTION_TYPE.SQL
**********************************************************************************/

/*********************************************************************************
* Start: J_HDW_GLINT_REF_SURVEY.SQL
**********************************************************************************/

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
tgttablename = 'edwhr.ref_survey';
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
FROM
  (SELECT DISTINCT survey_cycle_uuid
   FROM {{ params.param_hr_stage_dataset_name }}.glint_response)a
WHERE upper(trim(survey_cycle_uuid)) not in
    (SELECT upper(trim(survey_category_code))
     FROM {{ params.param_hr_core_dataset_name }}.ref_survey
     for system_time as of timestamp(tableload_start_time,'US/Central') 
     WHERE eff_to_date = DATE ("9999-12-31")
       AND upper(Trim(Source_System_Code)) = 'G')
       );

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.ref_survey
WHERE eff_to_date = DATE ("9999-12-31")
  AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE
  AND upper(Trim(Source_System_Code)) = 'G');

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

/*********************************************************************************
* End: J_HDW_GLINT_REF_SURVEY.SQL
**********************************************************************************/

/*********************************************************************************
* Start: J_HDW_GLINT_RESPONDENT_DETAIL.SQL
**********************************************************************************/

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
tgttablename = 'edwhr.respondent_detail';
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
FROM
  (
  SELECT
      CAST(substr(survey_creation_date, 1, 10) as DATE) AS survey_receive_date,
      rs.survey_sid,
      gr.employee_num
    FROM
      {{ params.param_hr_stage_dataset_name }}.glint_response AS gr
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON gr.survey_cycle_uuid = rs.survey_category_code
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.respondent_detail AS rd 
      for system_time as of timestamp(tableload_start_time,'US/Central')
      ON rd.employee_num = gr.employee_num
      AND rs.survey_sid = rd.survey_sid
    WHERE rd.employee_num IS NULL
      AND rd.survey_sid IS NULL
      AND (upper(gr.employee_num) LIKE 'UK%'
      OR upper(gr.employee_num) LIKE 'HEA%'
      OR substr(gr.employee_num, 1, 1) IN(
      '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
    ))
    GROUP BY 1, 2, 3
  ) a);

SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.respondent_detail
   WHERE upper(Trim(Source_System_Code)) = 'G'
     AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE )a);

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

/*********************************************************************************
* End: J_HDW_GLINT_RESPONDENT_DETAIL.SQL
**********************************************************************************/

/*********************************************************************************
* Start: J_HDW_GLINT_SURVEY_QUESTION.SQL
**********************************************************************************/

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
tgttablename = 'edwhr.survey_question';
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
FROM
(SELECT
        CAST(row_number() OVER (ORDER BY survey_cycle_uuid) as INT64 ) + (
          SELECT
              coalesce(max(survey_question_sid), CAST(0 as INT64 ))
            FROM
              {{ params.param_hr_core_dataset_name }}.survey_question
              for system_time as of timestamp(tableload_start_time,'US/Central') 
        ) AS survey_question_sid,
        CURRENT_DATE('US/Central') AS eff_from_date,
        coalesce(rs.survey_sid, 0) AS survey_sid,
        question_label AS question_id,
        rqt.question_type_code AS question_type_code,
        question_text AS question_desc,
        question_order AS question_seq_num,
        DATE ("9999-12-31") AS eff_to_date,
        'G' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.glint_question AS gq
        INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_survey AS rs ON rs.survey_category_code = gq.survey_cycle_uuid
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_question_type AS rqt ON gq.question_type = rqt.question_type_desc
        LEFT JOIN {{ params.param_hr_core_dataset_name }}.survey_question sq
        for system_time as of timestamp(tableload_start_time,'US/Central') 
        ON coalesce(rs.survey_sid, 0) = coalesce(sq.survey_sid, 0)
        AND sq.question_id = question_label
        AND sq.eff_to_date = DATE ("9999-12-31")
        AND Trim(sq.source_system_code) = 'G'
        WHERE sq.question_id IS NULL));

SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.survey_question
   WHERE upper(Trim(Source_System_Code)) = 'G'
     AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE )a);

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

/*********************************************************************************
* End: J_HDW_GLINT_SURVEY_QUESTION.SQL
**********************************************************************************/

/*********************************************************************************
* Start: J_HDW_GLINT_SURVEY_RESPONSE.SQL
**********************************************************************************/

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
tgttablename = 'edwhr.survey_response';
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
FROM
  (SELECT gr.*
   FROM {{ params.param_hr_stage_dataset_name }}.glint_response gr
   INNER JOIN {{ params.param_hr_core_dataset_name }}.ref_survey rs ON gr.survey_cycle_uuid = rs.survey_category_code
   INNER JOIN {{ params.param_hr_core_dataset_name }}.survey_question sq ON sq.survey_sid = rs.survey_sid
   AND sq.question_id = gr.question
   INNER JOIN {{ params.param_hr_core_dataset_name }}.respondent_detail rd ON rs.survey_category_code = gr.survey_cycle_uuid
   AND rs.survey_sid = rd.survey_sid
   AND rd.employee_num = gr.employee_num
   LEFT JOIN {{ params.param_hr_core_dataset_name }}.survey_response sr 
   for system_time as of timestamp(tableload_start_time,'US/Central') 
   ON sr.survey_question_sid = sq.survey_question_sid
   AND sr.respondent_id = rd.respondent_id
   AND sr.survey_receive_date = rd.survey_receive_date
   WHERE sr.survey_question_sid IS NULL
     AND sr.respondent_id IS NULL
     AND sr.survey_receive_date IS NULL
   QUALIFY ROW_NUMBER() OVER (PARTITION BY sq.survey_question_sid, rd.respondent_id, rd.survey_receive_date ORDER BY gr.response) = 1 ) a);

SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.survey_response
   WHERE upper(Trim(Source_System_Code)) = 'G'
     AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE )a);

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

/*********************************************************************************
* End: J_HDW_GLINT_SURVEY_RESPONSE.SQL
**********************************************************************************/