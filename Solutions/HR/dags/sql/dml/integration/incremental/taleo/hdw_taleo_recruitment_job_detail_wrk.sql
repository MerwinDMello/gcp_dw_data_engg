DECLARE current_dt DATETIME;
SET current_dt = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);


/*  Truncate Worktable Table     */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.recruitment_job_detail_wrk;

/*  Load Work Table with working Data */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.recruitment_job_detail_wrk (recruitment_job_sid, element_detail_entity_text, element_detail_type_text, element_detail_seq_num, valid_from_date, valid_to_date, element_detail_id, element_detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT
        rj.recruitment_job_sid,
        CAST(udf.udfdefinition_entity as STRING) AS element_detail_entity_text,
        TRIM(CAST(udf.udfdefinition_id AS STRING)) AS element_detail_type_text,
        udf.sequence AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        udf.udselement_number AS element_detail_id,
        CAST(udf.value as STRING) AS element_detail_value_text,
        'T' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_jobinformationudf AS udf
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON udf.jobinformation_number = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'T'
      GROUP BY 1, CAST(udf.udfdefinition_entity as STRING), CAST(udf.udfdefinition_id AS STRING), 4, 5, 6, 7, CAST(udf.value as STRING), 9, 10
    UNION ALL
    SELECT
        -- Special Program
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'SPECIAL_PROGRAM' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(rp.special_program_name) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
        ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_position AS rp ON rj.recruitment_position_sid = rp.recruitment_position_sid
      WHERE rp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rp.source_system_code) = 'B'
       AND rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(rp.special_program_name), '') <> ''
       -- QUALIFY Row_Number() Over (PARTITION BY APB."Position" ORDER BY APB.AsofTimestamp DESC) = 1
      GROUP BY 1, 4, 5, 6, 7, trim(rp.special_program_name), 9, 10
    UNION ALL
    SELECT
        -- Other Positions Reporting to this Position
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'OTHER POSITIONS REPORTING TO THIS POSITION' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ahb.hcaorgunitdivisionatsdescription) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS ahb ON ahb.hrorganizationunit = ajb.hrorganizationunit
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
         ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ahb.hcaorgunitdivisionatsdescription), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ahb.hcaorgunitdivisionatsdescription), 9, 10
    UNION ALL
    SELECT
        -- Who Immediate Supervisor Reports To
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'WHO IMMEDIATE SUPERVISOR REPORTS TO' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.hcajobrequisitionancestordirectsupervisorcode AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.hcajobrequisitionancestordirectsupervisorcode as STRING)), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, ajb.hcajobrequisitionancestordirectsupervisorcode, 9, 10
    UNION ALL
    SELECT
        -- Position Control Number
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'POSITION CONTROL NUMBER' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.position AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.position AS STRING)), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, ajb.position, 9, 10
    UNION ALL
    SELECT
        -- Number Hours per Pay period
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'NUMBER HOURS PER PAY PERIOD' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.fte AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.fte AS STRING)), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, ajb.fte, 9, 10
    UNION ALL
    SELECT
        -- 'Hiring Manager Title
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'HIRING MANAGER TITLE' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ahb.hcaorgunitlobatsdescription) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS ahb ON ahb.hrorganizationunit = ajb.hrorganizationunit
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ahb.hcaorgunitlobatsdescription), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ahb.hcaorgunitlobatsdescription), 9, 10
    UNION ALL
    SELECT
        -- Department Name
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'DEPARTMENT NAME' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ahb.description) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS ahb ON ahb.hrorganizationunit = ajb.hrorganizationunit
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ahb.description), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ahb.description), 9, 10
    UNION ALL
    SELECT
        -- Department Number
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'DEPARTMENT NUMBER' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.hrorganizationunit AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.hrorganizationunit AS STRING)), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, ajb.hrorganizationunit, 9, 10
    UNION ALL
    SELECT
        -- HR Company
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'HR COMPANY' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ajb.hrorganization) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ajb.hrorganization), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ajb.hrorganization), 9, 10
    UNION ALL
    SELECT
        -- BONUS_OP
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'BONUS_OP' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ajb.bonus) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ajb.bonus), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ajb.bonus), 9, 10
    UNION ALL
    SELECT
        -- JOB_GRADE
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'JOB_GRADE' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.salarystructuregrade AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.salarystructuregrade AS STRING)), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, ajb.salarystructuregrade, 9, 10
    UNION ALL
    SELECT
        -- Norm_Hours
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'NORM_HOURS' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        CAST(ajb.standardhours AS STRING) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(CAST(ajb.standardhours AS STRING)), '') <> ''
       AND cast(ajb.standardhours as float64) <> 0.00
      GROUP BY 1, 4, 5, 6, 7, ajb.standardhours, 9, 10
    UNION ALL
    SELECT
        -- Lawson_Job_Code
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'LAWSON_JOB_CODE' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(jc.job_code) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS ahbs ON ajb.hrorganizationunit = ahbs.hrorganizationunit
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS er 
		ON trim(ajb.hcajobrequisitionlawsonreqnumber) = trim(CAST(er.requisition_num AS STRING))
         AND cast(ahbs.hcaorgunitcompany as int64) = er.lawson_company_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON er.requisition_sid = rp.requisition_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON jp.position_sid = rp.position_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND er.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND rp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND jp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND jp.eff_to_date =  "9999-12-31"
       AND jc.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND coalesce(trim(jc.job_code), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(jc.job_code), 9, 10
    UNION ALL
    SELECT
        -- QUALIFY Row_Number() Over (PARTITION BY APB."Position" ORDER BY APB.AsofTimestamp DESC) = 1
        -- Lawson_Job_Description
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'LAWSON_JOB_DESCRIPTION' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(jc.job_code_desc) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ats_cust_hrorganizationunit_stg AS ahbs ON ajb.hrorganizationunit = ahbs.hrorganizationunit
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS er 
		ON trim(ajb.hcajobrequisitionlawsonreqnumber) = trim(CAST(er.requisition_num AS STRING))
         AND cast(ahbs.hcaorgunitcompany as int64) = er.lawson_company_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON er.requisition_sid = rp.requisition_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON jp.position_sid = rp.position_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND er.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND rp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND jp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND jp.eff_to_date =  "9999-12-31"
       AND jc.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND coalesce(trim(jc.job_code_desc), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(jc.job_code_desc), 9, 10
    UNION ALL
    SELECT
        -- QUALIFY Row_Number() Over (PARTITION BY APB."Position" ORDER BY APB.AsofTimestamp DESC) = 1
        -- Union_Code_2
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'UNION_CODE_2' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ajb._union) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ajb._union), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ajb._union), 9, 10
    UNION ALL
    SELECT
        -- MONTAGE_OTH_REQ_LUDS_UDF01
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'MONTAGE_OTH_REQ_LUDS_UDF01' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        trim(ajb.hcajobrequisitioninterviewtype) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ajb.hcajobrequisitioninterviewtype), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ajb.hcajobrequisitioninterviewtype), 9, 10
    UNION ALL
    SELECT
        -- MONTAGE_OTH_REQ_LUDS_UDF02
        rj.recruitment_job_sid,
        'JOBINFORMATION' AS element_detail_entity_text,
        'MONTAGE_OTH_REQ_LUDS_UDF02' AS element_detail_type_text,
        1 AS element_detail_seq_num,
        current_dt AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        NULL AS element_detail_id,
        substr(max(trim(ajb.hcajobrequisitioninterviewtypetemplate)), 1, 4000) AS element_detail_value_text,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS ajb
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj 
		ON cast(ajb.jobrequisition as int64) = rj.recruitment_job_num
      WHERE rj.valid_to_date =  DATETIME("9999-12-31 23:59:59")
       AND upper(rj.source_system_code) = 'B'
       AND coalesce(trim(ajb.hcajobrequisitioninterviewtypetemplate), '') <> ''
      GROUP BY 1, 4, 5, 6, 7, trim(ajb.hcajobrequisitioninterviewtypetemplate), 9, 10
  ;