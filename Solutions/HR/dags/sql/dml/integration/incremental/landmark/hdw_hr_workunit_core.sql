BEGIN
DECLARE current_ts datetime;
DECLARE dup_count int64;

SET current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.hr_workunit AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 second,
  dw_last_update_date_time = current_ts,
  active_dw_ind = 'N'
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk AS stg
WHERE
  tgt.workunit_sid = stg.workunit_sid
  AND (COALESCE(tgt.workunit_num, -999) <> COALESCE(stg.workunit_num, -999)
    OR COALESCE(TRIM(tgt.application_data_area_text), 'X') <> COALESCE(TRIM(stg.application_data_area_text), 'X')
    OR COALESCE(TRIM(tgt.application_key_text), 'X') <> COALESCE(TRIM(stg.application_key_text), 'X')
    OR COALESCE(TRIM(tgt.application_value_text), 'X') <> COALESCE(TRIM(stg.application_value_text), 'X')
    OR COALESCE(TRIM(tgt.service_definition_text), 'X') <> COALESCE(TRIM(stg.service_definition_text), 'X')
    OR COALESCE(TRIM(tgt.flow_definition_text), 'X') <> COALESCE(TRIM(stg.flow_definition_text), 'X')
    OR COALESCE(tgt.flow_version_num, -999) <> COALESCE(stg.flow_version_num, -999)
    OR COALESCE(TRIM(tgt.actor_id_text), 'X') <> COALESCE(TRIM(stg.actor_id_text), 'X')
    OR COALESCE(TRIM(tgt.authenticated_actor_text), 'X') <> COALESCE(TRIM(stg.authenticated_actor_text), 'X')
    OR COALESCE(TRIM(tgt.work_title_text), 'X') <> COALESCE(TRIM(stg.work_title_text), 'X')
    OR COALESCE(TRIM(tgt.filter_key_text), 'X') <> COALESCE(TRIM(stg.filter_key_text), 'X')
    OR COALESCE(TRIM(tgt.filter_value_text), 'X') <> COALESCE(TRIM(stg.filter_value_text), 'X')
    OR COALESCE(tgt.execution_start_date_time, date '1901-01-01') <> COALESCE(stg.execution_start_date_time, date '1901-01-01')
    OR COALESCE(tgt.start_date_time, date '1901-01-01') <> COALESCE(stg.start_date_time, date '1901-01-01')
    OR COALESCE(tgt.close_date_time, date '1901-01-01') <> COALESCE(stg.close_date_time, date '1901-01-01')
    OR COALESCE(tgt.status_id, -999) <> COALESCE(stg.status_id, -999)
    OR COALESCE(TRIM(tgt.criterion_1_id_text), 'X') <> COALESCE(TRIM(stg.criterion_1_id_text), 'X')
    OR COALESCE(TRIM(tgt.criterion_2_id_text), 'X') <> COALESCE(TRIM(stg.criterion_2_id_text), 'X')
    OR COALESCE(TRIM(tgt.criterion_3_id_text), 'X') <> COALESCE(TRIM(stg.criterion_3_id_text), 'X')
    OR COALESCE(TRIM(tgt.source_type_text), 'X') <> COALESCE(TRIM(stg.source_type_text), 'X')
    OR COALESCE(TRIM(tgt.source_1_text), 'X') <> COALESCE(TRIM(stg.source_1_text), 'X')
    OR COALESCE(TRIM(tgt.configuration_name), 'X') <> COALESCE(TRIM(stg.configuration_name), 'X')
    OR COALESCE(tgt.last_activity_num, -999) <> COALESCE(stg.last_activity_num, -999)
    OR COALESCE(tgt.last_message_num, -999) <> COALESCE(stg.last_message_num, -999)
    OR COALESCE(tgt.last_variable_seq_num, -999) <> COALESCE(stg.last_variable_seq_num, -999)
    OR COALESCE(tgt.classic_workunit_id, -999) <> COALESCE(stg.classic_workunit_id, -999)
    OR COALESCE(TRIM(tgt.classic_workunit_entered_ind), 'X') <> COALESCE(TRIM(stg.classic_workunit_entered_ind), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_1_text), 'X') <> COALESCE(TRIM(stg.key_field_value_1_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_2_text), 'X') <> COALESCE(TRIM(stg.key_field_value_2_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_3_text), 'X') <> COALESCE(TRIM(stg.key_field_value_3_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_4_text), 'X') <> COALESCE(TRIM(stg.key_field_value_4_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_5_text), 'X') <> COALESCE(TRIM(stg.key_field_value_5_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_6_text), 'X') <> COALESCE(TRIM(stg.key_field_value_6_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_7_text), 'X') <> COALESCE(TRIM(stg.key_field_value_7_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_8_text), 'X') <> COALESCE(TRIM(stg.key_field_value_8_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_9_text), 'X') <> COALESCE(TRIM(stg.key_field_value_9_text), 'X')
    OR COALESCE(TRIM(tgt.key_field_value_10_text), 'X') <> COALESCE(TRIM(stg.key_field_value_10_text), 'X')
    OR COALESCE(tgt.lawson_company_num, -999) <> COALESCE(stg.lawson_company_num, -999)
    OR COALESCE(TRIM(tgt.process_level_code), 'X') <> COALESCE(TRIM(stg.process_level_code), 'X')
    OR COALESCE(TRIM(tgt.source_system_code), 'X') <> COALESCE(TRIM(stg.source_system_code), 'X')
    OR COALESCE(TRIM(tgt.active_dw_ind), 'X') <> COALESCE(TRIM(stg.active_dw_ind), 'X'))
  AND tgt.valid_to_date = datetime '9999-12-31 23:59:59';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.hr_workunit (workunit_sid,
    valid_from_date,
    valid_to_date,
    workunit_num,
    application_data_area_text,
    application_key_text,
    application_value_text,
    service_definition_text,
    flow_definition_text,
    flow_version_num,
    actor_id_text,
    authenticated_actor_text,
    work_title_text,
    filter_key_text,
    filter_value_text,
    execution_start_date_time,
    start_date_time,
    close_date_time,
    status_id,
    criterion_1_id_text,
    criterion_2_id_text,
    criterion_3_id_text,
    source_type_text,
    source_1_text,
    configuration_name,
    last_activity_num,
    last_message_num,
    last_variable_seq_num,
    classic_workunit_id,
    classic_workunit_entered_ind,
    key_field_value_1_text,
    key_field_value_2_text,
    key_field_value_3_text,
    key_field_value_4_text,
    key_field_value_5_text,
    key_field_value_6_text,
    key_field_value_7_text,
    key_field_value_8_text,
    key_field_value_9_text,
    key_field_value_10_text,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.workunit_sid,
  current_ts,
  DATETIME("9999-12-31 23:59:59"),
  stg.workunit_num,
  stg.application_data_area_text,
  stg.application_key_text,
  stg.application_value_text,
  stg.service_definition_text,
  stg.flow_definition_text,
  stg.flow_version_num,
  stg.actor_id_text,
  stg.authenticated_actor_text,
  stg.work_title_text,
  stg.filter_key_text,
  stg.filter_value_text,
  stg.execution_start_date_time,
  stg.start_date_time,
  stg.close_date_time,
  stg.status_id,
  stg.criterion_1_id_text,
  stg.criterion_2_id_text,
  stg.criterion_3_id_text,
  stg.source_type_text,
  stg.source_1_text,
  stg.configuration_name,
  stg.last_activity_num,
  stg.last_message_num,
  stg.last_variable_seq_num,
  stg.classic_workunit_id,
  stg.classic_workunit_entered_ind,
  stg.key_field_value_1_text,
  stg.key_field_value_2_text,
  stg.key_field_value_3_text,
  stg.key_field_value_4_text,
  stg.key_field_value_5_text,
  stg.key_field_value_6_text,
  stg.key_field_value_7_text,
  stg.key_field_value_8_text,
  stg.key_field_value_9_text,
  stg.key_field_value_10_text,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.active_dw_ind,
  stg.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.hr_workunit AS tgt
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
  tgt.workunit_sid IS NULL ;

SET
  dup_count = (SELECT COUNT(*) FROM (
                                      SELECT workunit_sid, valid_from_date
                                      FROM
                                      {{ params.param_hr_core_dataset_name }}.hr_workunit
                                      GROUP BY workunit_sid, valid_from_date
                                      HAVING COUNT(*) > 1 
                                    ) 
              );
IF
  dup_count <> 0 THEN
  ROLLBACK TRANSACTION; raise
  USING
    message = CONCAT('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_workunit');
ELSE
  COMMIT TRANSACTION;
END IF;
END