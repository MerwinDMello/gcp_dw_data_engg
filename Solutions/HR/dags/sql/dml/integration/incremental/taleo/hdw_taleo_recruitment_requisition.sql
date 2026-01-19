BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);  

BEGIN TRANSACTION;	  

  /*  CLOSE THE PREVIOUS RECORDS FROM TARGET TABLE FOR SAME KEY FOR ANY CHANGES  */ 
  /*  INSERT THE NEW RECORDS/CHNAGES INTO THE TARGET TABLE  */ 
  /*  TRANSACTION BLOCK STARTS HERE */
UPDATE
  {{ params.param_hr_core_dataset_name }}.recruitment_requisition AS tgt
SET
  valid_to_date = (current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk AS stg
WHERE
  tgt.recruitment_requisition_sid = stg.recruitment_requisition_sid 
  AND (COALESCE(tgt.requisition_num, -999) <> COALESCE(stg.requisition_num, -999)
    OR COALESCE(tgt.lawson_requisition_sid, -9) <> COALESCE(stg.lawson_requisition_sid, -9)
    OR COALESCE(tgt.lawson_requisition_num, -9) <> COALESCE(stg.lawson_requisition_num, -9)
    OR COALESCE(tgt.hiring_manager_user_sid, -9) <> COALESCE(stg.hiring_manager_user_sid, -9)
    OR UPPER(TRIM(COALESCE(tgt.recruitment_requisition_num_text, '-999'))) <> UPPER(TRIM(COALESCE(stg.recruitment_requisition_num_text, '-999')))
    OR UPPER(TRIM(COALESCE(tgt.process_level_code, '-9'))) <> UPPER(TRIM(COALESCE(stg.process_level_code, '-9')))
    OR TRIM(CAST(COALESCE(tgt.approved_sw, -9) AS STRING)) <> TRIM(CAST(COALESCE(stg.approved_sw, -9) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.target_start_date, DATE '1900-01-01') AS STRING)) <> TRIM(CAST(COALESCE(stg.target_start_date, DATE '1900-01-01') AS STRING))
    OR COALESCE(tgt.required_asset_num, -999) <> COALESCE(stg.required_asset_num, -999)
    OR TRIM(CAST(COALESCE(tgt.required_asset_sw, -9) AS STRING)) <> TRIM(CAST(COALESCE(stg.required_asset_sw, -9) AS STRING))
    OR COALESCE(tgt.workflow_id, -999) <> COALESCE(stg.workflow_id, -999)
    OR COALESCE(tgt.recruitment_job_sid, -9) <> COALESCE(stg.recruitment_job_sid, -9)
    OR COALESCE(tgt.job_template_sid, -9) <> COALESCE(stg.job_template_sid, -9)
    OR COALESCE(TRIM(tgt.Requisition_New_Graduate_Flag),'') <> COALESCE(TRIM(stg.Requisition_New_Graduate_Flag),'')
    OR COALESCE(tgt.lawson_company_num, -9) <> COALESCE(stg.lawson_company_num, -9)
    OR COALESCE(TRIM(tgt.source_system_code), '') <> COALESCE(TRIM(stg.source_system_code), ''))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.recruitment_requisition (recruitment_requisition_sid,
    valid_from_date,
    valid_to_date,
    requisition_num,
    lawson_requisition_sid,
    lawson_requisition_num,
    hiring_manager_user_sid,
    recruitment_requisition_num_text,
    process_level_code,
    approved_sw,
    target_start_date,
    required_asset_num,
    required_asset_sw,
    workflow_id,
    recruitment_job_sid,
    job_template_sid,
    requisition_new_graduate_flag,
    lawson_company_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.recruitment_requisition_sid,
  current_dt AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") as valid_to_date,
  stg.requisition_num,
  stg.lawson_requisition_sid,
  stg.lawson_requisition_num,
  stg.hiring_manager_user_sid,
  stg.recruitment_requisition_num_text,
  stg.process_level_code,
  stg.approved_sw,
  stg.target_start_date,
  stg.required_asset_num,
  stg.required_asset_sw,
  stg.workflow_id,
  stg.recruitment_job_sid,
  stg.job_template_sid,
  stg.requisition_new_graduate_flag,
  stg.lawson_company_num,
  stg.source_system_code,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS tgt
ON
  stg.recruitment_requisition_sid = tgt.recruitment_requisition_sid
  AND COALESCE(tgt.requisition_num, -999) = COALESCE(stg.requisition_num, -999)
  AND COALESCE(tgt.lawson_requisition_sid, -9) = COALESCE(stg.lawson_requisition_sid, -9)
  AND COALESCE(tgt.lawson_requisition_num, -9) = COALESCE(stg.lawson_requisition_num, -9)
  AND COALESCE(tgt.hiring_manager_user_sid, -9) = COALESCE(stg.hiring_manager_user_sid, -9)
  AND COALESCE(CAST(tgt.recruitment_requisition_num_text AS STRING), '-999') = COALESCE(CAST(tgt.recruitment_requisition_num_text AS STRING), '-999')
  AND TRIM(CAST(COALESCE(tgt.process_level_code, '-9') AS STRING)) = TRIM(CAST(COALESCE(stg.process_level_code, '-9') AS STRING))
  AND TRIM(CAST(COALESCE(tgt.approved_sw, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.approved_sw, -9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.target_start_date, DATE '1900-01-01') AS STRING)) = TRIM(CAST(COALESCE(stg.target_start_date, DATE '1900-01-01') AS STRING))
  AND TRIM(CAST(COALESCE(tgt.required_asset_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.required_asset_num, -999) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.required_asset_sw, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.required_asset_sw, -9) AS STRING))
  AND COALESCE(tgt.workflow_id, -999) = COALESCE(stg.workflow_id, -999)
  AND COALESCE(tgt.recruitment_job_sid, -9) = COALESCE(stg.recruitment_job_sid, -9)
  AND COALESCE(tgt.job_template_sid, -9) = COALESCE(stg.job_template_sid, -9)
  AND COALESCE(TRIM(tgt.requisition_new_graduate_flag),'') =COALESCE(TRIM(stg.requisition_new_graduate_flag),'')
  AND COALESCE(tgt.lawson_company_num, -9) = COALESCE(stg.lawson_company_num, -9)
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.recruitment_requisition_sid IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.recruitment_requisition_sid, stg.valid_from_date ORDER BY stg.recruitment_requisition_sid) = 1 ;

    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            recruitment_requisition_sid ,valid_from_date 
        from {{ params.param_hr_core_dataset_name }}.recruitment_requisition
        group by recruitment_requisition_sid ,valid_from_date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_requisition');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;