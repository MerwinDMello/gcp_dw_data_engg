BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
/*  Close the previous records from Target table for same key for any Changes  */
/*  Insert the New Records/Chanages into the Target Table  */


  UPDATE {{ params.param_hr_core_dataset_name }}.requisition_workflow AS tgt SET active_dw_ind = 'N', valid_to_date = (current_ts - INTERVAL 1 second), dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk AS stg WHERE tgt.requisition_sid = stg.requisition_sid
   AND trim(cast(tgt.workflow_seq_num as string)) = trim(cast(stg.workflow_seq_num as string))
   AND upper(stg.source_system_code) = 'L'
   AND (upper((trim(CAST(coalesce(tgt.workflow_workunit_num_text, '0') as STRING)))) <> upper(trim(CAST(coalesce(stg.workflow_workunit_num_text, '0') as STRING)))
   OR trim(CAST(coalesce(tgt.workflow_activity_num, 0) as STRING)) <> trim(CAST(coalesce(stg.workflow_activity_num, 0) as STRING))
   OR upper(trim(coalesce(tgt.workflow_role_name, 'XX'))) <> upper(trim(coalesce(stg.workflow_role_name, 'XX')))
   OR upper(trim(coalesce(tgt.workflow_task_name, 'XX'))) <> upper(trim(coalesce(stg.workflow_task_name, 'XX')))
   OR trim(CAST(coalesce(tgt.start_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.start_date, DATE '1900-01-01') as STRING))
   OR coalesce(tgt.start_time, TIME '00:00:00') <> coalesce(stg.start_time, TIME '00:00:00')
   OR trim(CAST(coalesce(tgt.end_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.end_date, DATE '1900-01-01') as STRING))
   OR coalesce(tgt.end_time, TIME '00:00:00') <> coalesce(stg.end_time, TIME '00:00:00')
   OR trim(CAST(coalesce(tgt.workflow_user_id_code, '0') as STRING)) <> trim(CAST(coalesce(stg.workflow_user_id_code, '0') as STRING))
   OR trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) <> trim(CAST(coalesce(stg.lawson_company_num, 0) as STRING))
   OR upper(trim(coalesce(tgt.process_level_code, 'XX'))) <> upper(trim(coalesce(stg.process_level_code, 'XX')))
   OR upper(trim(coalesce(tgt.active_dw_ind, 'X'))) <> upper(trim(coalesce(stg.active_dw_ind, 'X'))))
   AND tgt.valid_to_date = DATETIME ('9999-12-31 23:59:59')
   AND tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.requisition_workflow AS tgt SET active_dw_ind = 'N', valid_to_date = (current_ts - INTERVAL 1 second), dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk AS stg WHERE trim(tgt.workflow_workunit_num_text) = trim(stg.workflow_workunit_num_text)
   AND tgt.workflow_activity_num = stg.workflow_activity_num
   AND upper(stg.source_system_code) = 'B'
   AND (upper(trim(coalesce(tgt.workflow_role_name, 'XX'))) <> upper(trim(coalesce(stg.workflow_role_name, 'XX')))
   OR upper(trim(coalesce(tgt.workflow_task_name, 'XX'))) <> upper(trim(coalesce(stg.workflow_task_name, 'XX')))
   OR trim(CAST(coalesce(tgt.start_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.start_date, DATE '1900-01-01') as STRING))
   OR coalesce(tgt.start_time, TIME '00:00:00') <> coalesce(stg.start_time, TIME '00:00:00')
   OR trim(CAST(coalesce(tgt.end_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.end_date, DATE '1900-01-01') as STRING))
   OR coalesce(tgt.end_time, TIME '00:00:00') <> coalesce(stg.end_time, TIME '00:00:00')
   OR trim(CAST(coalesce(tgt.workflow_user_id_code, '0') as STRING)) <> trim(CAST(coalesce(stg.workflow_user_id_code, '0') as STRING))
   OR trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) <> trim(CAST(coalesce(stg.lawson_company_num, 0) as STRING))
   OR upper(trim(coalesce(tgt.process_level_code, 'XX'))) <> upper(trim(coalesce(stg.process_level_code, 'XX')))
   OR upper(trim(coalesce(tgt.active_dw_ind, 'X'))) <> upper(trim(coalesce(stg.active_dw_ind, 'X'))))
   AND tgt.valid_to_date = DATETIME ('9999-12-31 23:59:59');

/* Begin Transaction Block Starts Here */
  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.requisition_workflow (requisition_sid, workflow_seq_num, valid_from_date, valid_to_date, workflow_workunit_num_text, workflow_activity_num, workflow_role_name, workflow_task_name, start_date, start_time, end_date, end_time, workflow_user_id_code, lawson_company_num, process_level_code, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.requisition_sid,
        stg.workflow_seq_num,
        current_ts,
        DATETIME ('9999-12-31 23:59:59'),
        trim(stg.workflow_workunit_num_text) AS workflow_workunit_num_text,
        stg.workflow_activity_num,
        trim(stg.workflow_role_name) AS workflow_role_name,
        trim(stg.workflow_task_name) AS workflow_task_name,
        stg.start_date,
        stg.start_time,
        stg.end_date,
        stg.end_time,
        trim(stg.workflow_user_id_code) AS workflow_user_id_code,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.active_dw_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition_workflow AS tgt ON tgt.requisition_sid = stg.requisition_sid
         AND trim(cast(tgt.workflow_seq_num as string)) = trim(cast(stg.workflow_seq_num as string))
         AND upper(trim(CAST(coalesce(tgt.workflow_workunit_num_text, '0') as STRING))) = upper(trim(CAST(coalesce(stg.workflow_workunit_num_text, '0') as STRING)))
         AND trim(CAST(coalesce(tgt.workflow_activity_num, 0) as STRING)) = trim(CAST(coalesce(stg.workflow_activity_num, 0) as STRING))
         AND upper(trim(coalesce(tgt.workflow_role_name, 'XX'))) = upper(trim(coalesce(stg.workflow_role_name, 'XX')))
         AND upper(trim(coalesce(tgt.workflow_task_name, 'XX'))) = upper(trim(coalesce(stg.workflow_task_name, 'XX')))
         AND trim(CAST(coalesce(tgt.start_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.start_date, DATE '1900-01-01') as STRING))
         AND coalesce(tgt.start_time, TIME '00:00:00') = coalesce(stg.start_time, TIME '00:00:00')
         AND trim(CAST(coalesce(tgt.end_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.end_date, DATE '1900-01-01') as STRING))
         AND coalesce(tgt.end_time, TIME '00:00:00') = coalesce(stg.end_time, TIME '00:00:00')
         AND trim(CAST(coalesce(tgt.workflow_user_id_code, '0') as STRING)) = trim(CAST(coalesce(stg.workflow_user_id_code, '0') as STRING))
         AND trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) = trim(CAST(coalesce(stg.lawson_company_num, 0) as STRING))
         AND upper(trim(coalesce(tgt.process_level_code, 'XX'))) = upper(trim(coalesce(stg.process_level_code, 'XX')))
         AND upper(trim(coalesce(tgt.active_dw_ind, 'X'))) = upper(trim(coalesce(stg.active_dw_ind, 'X')))
         AND tgt.valid_to_date = DATETIME ('9999-12-31 23:59:59')
      WHERE tgt.requisition_sid IS NULL
      QUALIFY row_number() OVER (PARTITION BY stg.requisition_sid, stg.workflow_seq_num, stg.valid_from_date ORDER BY stg.requisition_sid, stg.workflow_seq_num, stg.valid_from_date DESC) = 1
  ;

/*  RETIRE RECORD ON 2ND RETIRE LOGIC */

  UPDATE {{ params.param_hr_core_dataset_name }}.requisition_workflow AS tgt SET active_dw_ind = 'N', valid_to_date = datetime_sub(current_ts, interval 1 second), dw_last_update_date_time = current_ts WHERE tgt.valid_to_date = DATETIME( '9999-12-31 23:59:59')
   AND upper(tgt.source_system_code) = 'L'
   AND (tgt.requisition_sid, tgt.workflow_seq_num, tgt.workflow_workunit_num_text) NOT IN(
    SELECT AS STRUCT
        requisition_sid,
        workflow_seq_num,
        workflow_workunit_num_text
      FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk
      GROUP BY 1, 2, 3
  );

    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Requisition_SID ,Workflow_Seq_Num ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.requisition_workflow
        group by Requisition_SID ,Workflow_Seq_Num ,Valid_From_Date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: requisition_workflow' );
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;