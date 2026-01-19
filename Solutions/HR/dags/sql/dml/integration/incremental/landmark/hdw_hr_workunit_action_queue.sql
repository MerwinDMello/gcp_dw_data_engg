BEGIN 

DECLARE current_ts datetime;

DECLARE DUP_COUNT INT64;

SET
  current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.hr_workunit_action_queue AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = stg.dw_last_update_date_time,
  active_dw_ind = 'N'
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_action_queue_wrk AS stg
WHERE
  tgt.workunit_sid = stg.workunit_sid
  AND tgt.activity_seq_num = stg.activity_seq_num
  AND (
    coalesce(tgt.workunit_num, -999) <> coalesce(stg.workunit_num, -999)
    OR coalesce(trim(tgt.work_desc), 'X') <> coalesce(trim(stg.work_desc), 'X')
    OR coalesce(trim(tgt.action_taken_text), 'X') <> coalesce(trim(stg.action_taken_text), 'X')
    OR coalesce(tgt.display_type_num, -999) <> coalesce(stg.display_type_num, -999)
    OR coalesce(trim(tgt.display_name), 'X') <> coalesce(trim(stg.display_name), 'X')
    OR coalesce(trim(tgt.filter_key_text), 'X') <> coalesce(trim(stg.filter_key_text), 'X')
    OR coalesce(trim(tgt.filter_value_num_text), 'X') <> coalesce(trim(stg.filter_value_num_text), 'X')
    OR coalesce(tgt.time_out_type_num, -999) <> coalesce(stg.time_out_type_num, -999)
    OR coalesce(tgt.time_out_hour_num, -999) <> coalesce(stg.time_out_hour_num, -999)
    OR coalesce(trim(tgt.time_out_action_text), 'X') <> coalesce(trim(stg.time_out_action_text), 'X')
    OR coalesce(tgt.timed_out_sw, -999) <> coalesce(stg.timed_out_sw, -999)
    OR coalesce(tgt.last_queue_action_num, -999) <> coalesce(stg.last_queue_action_num, -999)
    OR coalesce(trim(tgt.configurator_name), 'X') <> coalesce(trim(stg.configurator_name), 'X')
    OR coalesce(tgt.lawson_company_num, -999) <> coalesce(stg.lawson_company_num, -999)
    OR coalesce(trim(tgt.process_level_code), 'X') <> coalesce(trim(stg.process_level_code), 'X')
    OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X')
    OR coalesce(trim(tgt.active_dw_ind), 'X') <> coalesce(trim(stg.active_dw_ind), 'X')
  )
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.hr_workunit_action_queue (
    workunit_sid,
    activity_seq_num,
    valid_from_date,
    valid_to_date,
    workunit_num,
    work_desc,
    action_taken_text,
    display_type_num,
    display_name,
    filter_key_text,
    filter_value_num_text,
    time_out_type_num,
    time_out_hour_num,
    time_out_action_text,
    timed_out_sw,
    last_queue_action_num,
    configurator_name,
    lawson_company_num,
    process_level_code,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  stg.workunit_sid,
  stg.activity_seq_num,
  current_ts,
  DATETIME("9999-12-31 23:59:59"),
  stg.workunit_num,
  stg.work_desc,
  stg.action_taken_text,
  stg.display_type_num,
  stg.display_name,
  stg.filter_key_text,
  stg.filter_value_num_text,
  stg.time_out_type_num,
  stg.time_out_hour_num,
  stg.time_out_action_text,
  stg.timed_out_sw,
  stg.last_queue_action_num,
  stg.configurator_name,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.active_dw_ind,
  stg.source_system_code,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_workunit_action_queue_wrk AS stg
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_action_queue AS tgt ON stg.workunit_sid = tgt.workunit_sid
  AND tgt.activity_seq_num = stg.activity_seq_num
  AND coalesce(tgt.workunit_num, -999) = coalesce(stg.workunit_num, -999)
  AND coalesce(trim(tgt.work_desc), 'X') = coalesce(trim(stg.work_desc), 'X')
  AND coalesce(trim(tgt.action_taken_text), 'X') = coalesce(trim(stg.action_taken_text), 'X')
  AND coalesce(tgt.display_type_num, -999) = coalesce(stg.display_type_num, -999)
  AND coalesce(trim(tgt.display_name), 'X') = coalesce(trim(stg.display_name), 'X')
  AND coalesce(trim(tgt.filter_key_text), 'X') = coalesce(trim(stg.filter_key_text), 'X')
  AND coalesce(trim(tgt.filter_value_num_text), 'X') = coalesce(trim(stg.filter_value_num_text), 'X')
  AND coalesce(tgt.time_out_type_num, -999) = coalesce(stg.time_out_type_num, -999)
  AND coalesce(tgt.time_out_hour_num, -999) = coalesce(stg.time_out_hour_num, -999)
  AND coalesce(trim(tgt.time_out_action_text), 'X') = coalesce(trim(stg.time_out_action_text), 'X')
  AND coalesce(tgt.timed_out_sw, -999) = coalesce(stg.timed_out_sw, -999)
  AND coalesce(tgt.last_queue_action_num, -999) = coalesce(stg.last_queue_action_num, -999)
  AND coalesce(trim(tgt.configurator_name), 'X') = coalesce(trim(stg.configurator_name), 'X')
  AND coalesce(tgt.lawson_company_num, -999) = coalesce(stg.lawson_company_num, -999)
  AND coalesce(trim(tgt.process_level_code), 'X') = coalesce(trim(stg.process_level_code), 'X')
  AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
  AND coalesce(trim(tgt.active_dw_ind), 'X') = coalesce(trim(stg.active_dw_ind), 'X')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.workunit_sid IS NULL;

/* Test Unique Primary Index constarint set in Teradata*/
SET
  DUP_COUNT =(
    select
      count(*)
    from
      (
        select
          Workunit_SID,
          Activity_Seq_Num,
          Valid_From_Date
        from
          {{ params.param_hr_core_dataset_name }}.hr_workunit_action_queue
        group by
          Workunit_SID,
          Activity_Seq_Num,
          Valid_From_Date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table : edwhr_copy.hr_workunit_action_queue'
);

ELSE COMMIT TRANSACTION;

END IF;

END;