/*  Truncate Worktable Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.junc_employee_recruitment_user_wrk; 

  /*  Load Work Table with working Data */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.junc_employee_recruitment_user_wrk (employee_sid,
    recruitment_user_sid,
    valid_from_date,
    primary_facility_ind,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time) (
  SELECT
    emp.employee_sid AS employee_sid,
    ru.recruitment_user_sid AS recruitment_user_sid,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
    -- changed CURRENT date TO File_Date,    nb
     emp.primary_facility_ind AS primary_facility_ind,
    DATETIME("9999-12-31 23:59:59") AS valid_to_date,
    'T' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_user AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.employee AS emp
  ON
    TRIM(COALESCE(stg.employeeid, 'X')) = cast(emp.employee_num as string)
    AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru
  ON
    stg.userno = ru.recruitment_user_num
  WHERE
    (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  UNION ALL
  SELECT
    emp.employee_sid AS employee_sid,
    ru.recruitment_user_sid AS recruitment_user_sid,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
    emp.primary_facility_ind AS primary_facility_ind,
    DATETIME("9999-12-31 23:59:59") AS valid_to_date,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
  FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_recruiter_stg AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.employee AS emp
  ON
    
    COALESCE(cast(stg.recruiter as string),'X')=cast(emp.employee_num as string)
    AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru
  ON
    stg.recruiter = ru.recruitment_user_num 
  WHERE
    (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59") )
UNION DISTINCT
SELECT
  DISTINCT emp.employee_sid AS employee_sid,
  ru.recruitment_user_sid AS recruitment_user_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  emp.primary_facility_ind AS primary_facility_ind,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp
ON
  
  COALESCE(cast(stg.hiringmanager as string),'X')=cast(emp.employee_num as string)
  AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru
ON
  stg.hiringmanager = ru.recruitment_user_num
WHERE
  (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59")
UNION DISTINCT
SELECT
  DISTINCT emp.employee_sid AS employee_sid,
  ru.recruitment_user_sid AS recruitment_user_sid,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS valid_from_date,
  emp.primary_facility_ind AS primary_facility_ind,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  'B' AS source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.ats_cust_jobrequisition_stg AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp
ON
  
  COALESCE(cast(stg.creator as string),'X')=cast(emp.employee_num as string)
  
  AND (emp.valid_to_date) = DATETIME("9999-12-31 23:59:59")
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru
ON
  stg.creator = ru.recruitment_user_num 
WHERE
  (ru.valid_to_date) = DATETIME("9999-12-31 23:59:59") ;