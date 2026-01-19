
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
tgttablename = concat('edwhr.',@p_table);
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
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.hr_workunit AS tgt
  for system_time as of timestamp(tableload_start_time,'US/Central') 
ON
  stg.workunit_sid = tgt.workunit_sid
  AND COALESCE(tgt.workunit_num, -999) = COALESCE(stg.workunit_num, -999)
  AND COALESCE(TRIM(tgt.application_data_area_text), 'X') = COALESCE(TRIM(stg.application_data_area_text), 'X')
  AND COALESCE(TRIM(tgt.application_key_text), 'X') = COALESCE(TRIM(stg.application_key_text), 'X')
  AND COALESCE(TRIM(tgt.application_value_text), 'X') = COALESCE(TRIM(stg.application_value_text), 'X')
  AND COALESCE(TRIM(tgt.service_definition_text), 'X') = COALESCE(TRIM(stg.service_definition_text), 'X')
  AND COALESCE(TRIM(tgt.flow_definition_text), 'X') = COALESCE(TRIM(stg.flow_definition_text), 'X')
  AND COALESCE(tgt.flow_version_num, -999) = COALESCE(stg.flow_version_num, -999)
  AND COALESCE(TRIM(tgt.actor_id_text), 'X') = COALESCE(TRIM(stg.actor_id_text), 'X')
  AND COALESCE(TRIM(tgt.authenticated_actor_text), 'X') = COALESCE(TRIM(stg.authenticated_actor_text), 'X')
  AND COALESCE(TRIM(tgt.work_title_text), 'X') = COALESCE(TRIM(stg.work_title_text), 'X')
  AND COALESCE(TRIM(tgt.filter_key_text), 'X') = COALESCE(TRIM(stg.filter_key_text), 'X')
  AND COALESCE(TRIM(tgt.filter_value_text), 'X') = COALESCE(TRIM(stg.filter_value_text), 'X')
  AND COALESCE(tgt.execution_start_date_time, date '1901-01-01') = COALESCE(stg.execution_start_date_time, date '1901-01-01')
  AND COALESCE(tgt.start_date_time, date '1901-01-01') = COALESCE(stg.start_date_time, date '1901-01-01')
  AND COALESCE(tgt.close_date_time, date '1901-01-01') = COALESCE(stg.close_date_time, date '1901-01-01')
  AND COALESCE(tgt.status_id, -999) = COALESCE(stg.status_id, -999)
  AND COALESCE(TRIM(tgt.criterion_1_id_text), 'X') = COALESCE(TRIM(stg.criterion_1_id_text), 'X')
  AND COALESCE(TRIM(tgt.criterion_2_id_text), 'X') = COALESCE(TRIM(stg.criterion_2_id_text), 'X')
  AND COALESCE(TRIM(tgt.criterion_3_id_text), 'X') = COALESCE(TRIM(stg.criterion_3_id_text), 'X')
  AND COALESCE(TRIM(tgt.source_type_text), 'X') = COALESCE(TRIM(stg.source_type_text), 'X')
  AND COALESCE(TRIM(tgt.source_1_text), 'X') = COALESCE(TRIM(stg.source_1_text), 'X')
  AND COALESCE(TRIM(tgt.configuration_name), 'X') = COALESCE(TRIM(stg.configuration_name), 'X')
  AND COALESCE(tgt.last_activity_num, -999) = COALESCE(stg.last_activity_num, -999)
  AND COALESCE(tgt.last_message_num, -999) = COALESCE(stg.last_message_num, -999)
  AND COALESCE(tgt.last_variable_seq_num, -999) = COALESCE(stg.last_variable_seq_num, -999)
  AND COALESCE(tgt.classic_workunit_id, -999) = COALESCE(stg.classic_workunit_id, -999)
  AND COALESCE(TRIM(tgt.classic_workunit_entered_ind), 'X') = COALESCE(TRIM(stg.classic_workunit_entered_ind), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_1_text), 'X') = COALESCE(TRIM(stg.key_field_value_1_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_2_text), 'X') = COALESCE(TRIM(stg.key_field_value_2_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_3_text), 'X') = COALESCE(TRIM(stg.key_field_value_3_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_4_text), 'X') = COALESCE(TRIM(stg.key_field_value_4_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_5_text), 'X') = COALESCE(TRIM(stg.key_field_value_5_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_6_text), 'X') = COALESCE(TRIM(stg.key_field_value_6_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_7_text), 'X') = COALESCE(TRIM(stg.key_field_value_7_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_8_text), 'X') = COALESCE(TRIM(stg.key_field_value_8_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_9_text), 'X') = COALESCE(TRIM(stg.key_field_value_9_text), 'X')
  AND COALESCE(TRIM(tgt.key_field_value_10_text), 'X') = COALESCE(TRIM(stg.key_field_value_10_text), 'X')
  AND COALESCE(tgt.lawson_company_num, -999) = COALESCE(stg.lawson_company_num, -999)
  AND COALESCE(TRIM(tgt.process_level_code), 'X') = COALESCE(TRIM(stg.process_level_code), 'X')
  AND COALESCE(TRIM(tgt.source_system_code), 'X') = COALESCE(TRIM(stg.source_system_code), 'X')
  AND COALESCE(TRIM(tgt.active_dw_ind), 'X') = COALESCE(TRIM(stg.active_dw_ind), 'X')
  AND DATE(tgt.valid_to_date) = DATE '9999-12-31'
WHERE
  tgt.workunit_sid IS NULL );

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.hr_workunit
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE
  AND active_dw_ind='Y');

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
