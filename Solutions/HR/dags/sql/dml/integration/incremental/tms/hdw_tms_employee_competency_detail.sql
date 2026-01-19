BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_dt datetime;
SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","competency_ratings_report","Coalesce(Trim(comp_record_id),'')","Employee_Competency_Detail");

TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.competency_ratings_report_reject; 
  /* INSERTING INTO THE REJECT TABLE 'EMPLOYEE_ID NOT IN EMPLOYEE_TALENT_PROFILE'*/ ---
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.competency_ratings_report_reject;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.competency_ratings_report_reject
SELECT
  stg.employee_id,
  stg.review_period,
  stg.review_period_start_date,
  stg.review_period_end_date,
  stg.review_year,
  stg.plan_name,
  stg.competency_group,
  stg.competency,
  stg.employee_rating_numeric_value,
  stg.employee_rating_scale_value,
  stg.manager_rating_numeric_value,
  stg.manager_rating_scale_value,
  stg.manager_employee_gap,
  stg.evaluation_workflow_state,
  stg.comp_record_id,
  'EMPLOYEE_ID NOT IN EMPLOYEE' AS reject_reason,
  'COMPETENCY_RATINGS_REPORT' AS reject_stg_tbl_nm,
  datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.competency_ratings_report AS stg
WHERE
  (stg.employee_id) NOT IN( 
    
    SELECT
      CAST(employee_num AS STRING) AS employee_num
    FROM
      {{ params.param_hr_core_dataset_name }}.employee ) ;
  TRUNCATE TABLE
    {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk; --
  TRUNCATE TABLE
    {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk;
  INSERT INTO
    {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk (employee_competency_result_sid,
      employee_talent_profile_sid,
      employee_sid,
      employee_num,
      performance_plan_id,
      competency_group_id,
      competency_id,
      evaluation_workflow_status_id,
      review_period_id,
      review_year_num,
      review_period_start_date,
      review_period_end_date,
      employee_rating_num,
      employee_rating_id,
      manager_rating_num,
      manager_rating_id,
      manager_employee_rating_gap_num,
      lawson_company_num,
      process_level_code,
      source_system_key,
      source_system_code,
      dw_last_update_date_time)
  SELECT
    CAST(xwlk.sk AS INT64) AS employee_competency_result_sid,
    etp.employee_talent_profile_sid AS employee_talent_profile_sid,
    emp.employee_sid AS employee_sid,
    CAST(stg.employee_id AS INT64) AS employee_num,
    rpp1.performance_plan_id AS performance_plan_id,
    rcg.competency_group_id AS competency_group_id,
    rc.competency_id AS competency_id,
    rps.performance_status_id AS evaluation_workflow_status_id,
    rpp2.review_period_id AS review_period_id,
    CAST(stg.review_year AS INT64) AS review_year_num,
    stg.review_period_start_date AS review_period_start_date,
    stg.review_period_end_date AS review_period_end_date,
    CAST(CASE
        WHEN UPPER(stg.employee_rating_numeric_value) = 'NOT RATED' OR UPPER(stg.employee_rating_numeric_value) = 'NOT APPLICABLE' THEN NULL
      ELSE
      REGEXP_EXTRACT(stg.employee_rating_numeric_value, r'^[0-9]+')
    END
      AS INT64) AS employee_rating_num,
    rpr.performance_rating_id AS employee_rating_id,
    CAST(CASE
        WHEN UPPER(stg.manager_rating_numeric_value) = 'NOT RATED' OR UPPER(stg.manager_rating_numeric_value) = 'NOT APPLICABLE' THEN NULL
      ELSE
      REGEXP_EXTRACT(stg.manager_rating_numeric_value, r'^[0-9]+')
    END
      AS INT64) AS manager_rating_num,
    rpr2.performance_rating_id AS manager_rating_id,
    CAST(CASE
        WHEN UPPER(stg.manager_employee_gap) = 'NOT COMPUTED' THEN NULL
      ELSE
      REGEXP_EXTRACT(stg.manager_employee_gap, r'^[0-9|-]+')
    END
      AS INT64) AS manager_employee_rating_gap_num,
    etp.lawson_company_num AS lawson_company_num,
    -- ETP.PROCESS_LEVEL_CODE AS PROCESS_LEVEL_CODE,
    COALESCE(etp.process_level_code, '00000') AS process_level_code,
    stg.comp_record_id AS source_system_key,
    'M' AS source_system_code,
    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report AS stg
  INNER JOIN
    {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk
  ON
    TRIM(stg.comp_record_id) = xwlk.sk_source_txt
    AND UPPER(xwlk.sk_type) = 'EMPLOYEE_COMPETENCY_DETAIL'
  INNER JOIN
    {{ params.param_hr_core_dataset_name }}.employee AS emp
  ON
    stg.employee_id = CAST(emp.employee_num AS STRING)
    AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
    AND emp.lawson_company_num =
    CASE SUBSTR(stg.job_code, 1, 4)
      WHEN '' THEN 0
    ELSE
    CAST(SUBSTR(stg.job_code, 1, 4) AS INT64)
  END
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.employee_talent_profile AS etp
  ON
    (etp.employee_num) = CAST(stg.employee_id AS INT64)
    AND (etp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_performance_plan AS rpp1
  ON
    UPPER(TRIM(rpp1.performance_plan_desc)) = UPPER(TRIM(stg.plan_name))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_competency_group AS rcg
  ON
    UPPER(TRIM(rcg.competency_group_desc)) = UPPER(TRIM(stg.competency_group))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_competency AS rc
  ON
    UPPER(TRIM(rc.competency_desc)) = UPPER(TRIM(stg.competency))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_performance_status AS rps
  ON
    UPPER(TRIM(rps.performance_status_desc)) = UPPER(TRIM(stg.evaluation_workflow_state))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_performance_period AS rpp2
  ON
    UPPER(TRIM(rpp2.review_period_desc)) = UPPER(TRIM(stg.review_period))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS rpr
  ON
    UPPER(TRIM(rpr.performance_rating_desc)) = UPPER(TRIM(stg.employee_rating_scale_value))
  LEFT OUTER JOIN
    {{ params.param_hr_core_dataset_name }}.ref_performance_rating AS rpr2
  ON
    UPPER(TRIM(rpr2.performance_rating_desc)) = UPPER(TRIM(stg.manager_rating_scale_value)) 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_competency_result_sid ORDER BY employee_competency_result_sid) = 1 ;
  BEGIN TRANSACTION;
  UPDATE
    {{ params.param_hr_core_dataset_name }}.employee_competency_detail AS tgt
  SET
    valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
  FROM (
    SELECT
      employee_competency_detail_wrk.*
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk ) AS wrk
  WHERE
    tgt.employee_competency_result_sid = wrk.employee_competency_result_sid
    AND (TRIM(CAST(COALESCE(tgt.employee_talent_profile_sid, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.employee_talent_profile_sid, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.performance_plan_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.performance_plan_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.employee_sid, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.employee_sid, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.employee_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.employee_num, 0) AS STRING)) --OR TRIM(COALESCE(tgt.competency_group_id, '')) <> TRIM(COALESCE(wrk.competency_group_id, ''))
      OR TRIM(CAST(COALESCE(tgt.competency_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.competency_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.evaluation_workflow_status_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.evaluation_workflow_status_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.review_period_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.review_period_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.review_year_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.review_year_num, 0) AS STRING))
      OR Coalesce(tgt.review_period_start_date, DATE '9999-01-01') <> Coalesce(wrk.review_period_start_date, DATE '9999-01-01')
      OR Coalesce(tgt.review_period_end_date, DATE '9999-01-01') <> Coalesce(wrk.review_period_end_date, DATE '9999-01-01')
      OR TRIM(CAST(COALESCE(tgt.employee_rating_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.employee_rating_num, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.employee_rating_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.employee_rating_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.manager_rating_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.manager_rating_num, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.manager_rating_id, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.manager_rating_id, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.manager_employee_rating_gap_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.manager_employee_rating_gap_num, 0) AS STRING))
      OR TRIM(CAST(COALESCE(tgt.lawson_company_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.lawson_company_num, 0) AS STRING)) 
      --OR TRIM(CAST(COALESCE(tgt.process_level_code, 0) AS STRING)) <> TRIM(CAST(COALESCE(wrk.process_level_code, 0) AS STRING))
      OR TRIM(COALESCE(tgt.source_system_key, '')) <> TRIM(COALESCE(wrk.source_system_key, '')))
    AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
	
	
  INSERT INTO
    {{ params.param_hr_core_dataset_name }}.employee_competency_detail (employee_competency_result_sid,
      valid_from_date,
      employee_sid,
      employee_num,
      employee_talent_profile_sid,
      performance_plan_id,
      competency_group_id,
      competency_id,
      evaluation_workflow_status_id,
      review_period_id,
      review_year_num,
      review_period_start_date,
      review_period_end_date,
      employee_rating_num,
      employee_rating_id,
      manager_rating_num,
      manager_rating_id,
      manager_employee_rating_gap_num,
      lawson_company_num,
      process_level_code,
      valid_to_date,
      source_system_key,
      source_system_code,
      dw_last_update_date_time)
  SELECT
    wrk.employee_competency_result_sid,
    current_dt AS valid_from_date,
    wrk.employee_sid,
    wrk.employee_num,
    wrk.employee_talent_profile_sid,
    wrk.performance_plan_id,
    wrk.competency_group_id,
    wrk.competency_id,
    wrk.evaluation_workflow_status_id,
    wrk.review_period_id,
    wrk.review_year_num,
    wrk.review_period_start_date,
    wrk.review_period_end_date,
    wrk.employee_rating_num,
    wrk.employee_rating_id,
    wrk.manager_rating_num,
    wrk.manager_rating_id,
    wrk.manager_employee_rating_gap_num,
    wrk.lawson_company_num,
    wrk.process_level_code,
    -- COALESCE(WRK.PROCESS_LEVEL_CODE,'00000') AS PROCESS_LEVEL_CODE,
    DATETIME("9999-12-31 23:59:59")  AS valid_to_date,
    TRIM(wrk.source_system_key),
    wrk.source_system_code,
    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk AS wrk
   WHERE
    ( COALESCE(wrk.employee_competency_result_sid,0),
      COALESCE(wrk.employee_talent_profile_sid,0),
      COALESCE(wrk.employee_sid,0),
      COALESCE(wrk.employee_num,0),
      COALESCE(wrk.performance_plan_id,0),
      COALESCE(wrk.competency_group_id,0),
      COALESCE(wrk.competency_id,0),
      COALESCE(wrk.evaluation_workflow_status_id,0),
      COALESCE(wrk.review_period_id,0),
      COALESCE(wrk.review_year_num,0),
      COALESCE(wrk.review_period_start_date, DATE '9999-01-01'),
      COALESCE(wrk.review_period_end_date, DATE '9999-01-01'),
      COALESCE(wrk.employee_rating_num,0),
      COALESCE(wrk.employee_rating_id,0),
      COALESCE(wrk.manager_rating_num,0),
      COALESCE(wrk.manager_rating_id,0),
      COALESCE(wrk.manager_employee_rating_gap_num,0),
      COALESCE(wrk.lawson_company_num,0),
      COALESCE(Trim(wrk.process_level_code),''),
      COALESCE(TRIM(wrk.source_system_key),''),
      COALESCE(wrk.source_system_code,'M')) NOT IN
      (
    SELECT
      AS STRUCT COALESCE(tgt.employee_competency_result_sid,0),
      COALESCE(tgt.employee_talent_profile_sid,0),
      COALESCE(tgt.employee_sid,0),
      COALESCE(tgt.employee_num,0),
      COALESCE(tgt.performance_plan_id,0),
      COALESCE(tgt.competency_group_id,0),
      COALESCE(tgt.competency_id,0),
      COALESCE(tgt.evaluation_workflow_status_id,0),
      COALESCE(tgt.review_period_id,0),
      COALESCE(tgt.review_year_num,0),
      COALESCE(tgt.review_period_start_date, DATE '9999-01-01'),
      COALESCE(tgt.review_period_end_date, DATE '9999-01-01'),
      COALESCE(tgt.employee_rating_num,0),
      COALESCE(tgt.employee_rating_id,0),
      COALESCE(tgt.manager_rating_num,0),
      COALESCE(tgt.manager_rating_id,0),
      COALESCE(tgt.manager_employee_rating_gap_num,0),
      COALESCE(tgt.lawson_company_num,0),
      COALESCE(trim(tgt.process_level_code),''),
      COALESCE(TRIM(tgt.source_system_key),''),
      COALESCE(trim(tgt.source_system_code),'M')
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_competency_detail AS tgt
    WHERE
      (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59") ) ;
  SET
    DUP_COUNT = (
    SELECT
      COUNT(*)
    FROM (
      SELECT
        Employee_Competency_Result_SID,
        Valid_From_Date
      FROM
        {{ params.param_hr_core_dataset_name }}.employee_competency_detail
      GROUP BY
        Employee_Competency_Result_SID,
        Valid_From_Date
      HAVING
        COUNT(*) > 1 ) );
  IF
    DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION; RAISE
  USING
    MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_competency_detail');
  END IF
    ;
  UPDATE
    {{ params.param_hr_core_dataset_name }}.employee_competency_detail AS tgt
  SET
    valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
  WHERE
    tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
    AND tgt.employee_competency_result_sid NOT IN(
    SELECT
      DISTINCT employee_competency_detail_wrk.employee_competency_result_sid
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee_competency_detail_wrk );
  COMMIT TRANSACTION;
  END;
