BEGIN
DECLARE dup_count INT64;
DECLARE current_dt DATETIME;
SET current_dt = datetime_trunc(current_datetime('US/Central'), SECOND);

  CALL `{{ params.param_hr_core_dataset_name }}.sk_gen`("{{ params.param_hr_stage_dataset_name }}","employee_perf_goals","Trim(individual_goal_id)","Employee_Goal_Detail");

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_perf_goals_reject;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_perf_goals_reject 
    SELECT
        stg.employee_id,
        stg.goal_title,
        stg.goal_weight,
        stg.goal_category,
        stg.expected_result,
        stg.measure,
        stg.due_date,
        stg.user_defined_date_1,
        stg.goal_status,
        stg.goal_progress,
        stg.emp_goal_rating,
        stg.emp_goal_rating_numeric_value,
        stg.mgr_goal_rating,
        stg.mgr_goal_rating_numeric_value,
        stg.plan_name,
        stg.review_period,
        stg.review_period_end_date,
        stg.review_period_start_date,
        stg.year_1,
        stg.individual_goal_id,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
        CASE
          WHEN trim(stg.employee_id) IS NULL THEN 'Employee_ID is NULL'
          WHEN substr(trim(stg.employee_id), 1, 1) NOT IN(
            '1', '2', '3', '4', '5', '6', '7', '8', '9'
          ) THEN 'Employee_Id is alpha_numeric'
          ELSE CAST(0 as STRING)
        END AS reject_reason,
        'Development_Activities_Report' AS reject_stg_tbl_nm
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_perf_goals AS stg
      WHERE substr(trim(stg.employee_id), 1, 1) NOT IN('1', '2', '3', '4', '5', '6', '7', '8', '9')
            OR ( substr(trim(stg.employee_id), 1, 1) IN('1', '2', '3', '4', '5', '6', '7', '8', '9')
                 AND CASE COALESCE(trim(stg.employee_id),'') 
                     WHEN '' THEN 0
                     ELSE CAST(trim(stg.employee_id) as INT64)
                     END NOT IN( SELECT employee_num
                                 FROM {{ params.param_hr_base_views_dataset_name }}.employee
                               )
               )
  ;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_goal_detail_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_goal_detail_wrk (employee_goal_detail_sid, employee_talent_profile_sid, employee_num, employee_sid, employee_goal_year_num, goal_name, goal_category_id, goal_weight_pct, expected_result_text, goal_measurement_text, goal_status_id, goal_progress_status_id, goal_performance_plan_id, goal_due_date, user_defined_date, review_year_num, review_period_end_date, review_period_start_date, review_period_id, manager_goal_performance_rating_id, manager_goal_performance_rating_num, employee_goal_performance_rating_id, employee_goal_performance_rating_num, lawson_company_num, process_level_code, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(xwlk.sk AS INT64) AS employee_goal_detail_sid,
        CAST(coalesce(etp.employee_talent_profile_sid, 0) as INT64) AS employee_talent_profile_sid,
        CAST(stg.employee_id AS INT64) AS employee_num,
        emp.employee_sid AS employee_sid,
        CAST(stg.year_1 AS INT64) AS employee_goal_year_num,
        trim(stg.goal_title) AS goal_name,
        rpc.performance_category_id AS goal_category_id,
        CAST(stg.goal_weight AS INT64) AS goal_weight_pct,
        stg.expected_result AS expected_result_text,
        stg.measure AS goal_measurement_text,
        rps.performance_status_id AS goal_status_id,
        rps2.performance_status_id AS goal_progress_status_id,
        rpn.performance_plan_id AS goal_performance_plan_id,
        stg.due_date AS goal_due_date,
        stg.user_defined_date_1 AS user_defined_date,
        CAST(stg.year_1 AS INT64) AS review_year_num,
        stg.review_period_end_date AS review_period_end_date,
        stg.review_period_start_date AS review_period_start_date,
        rpp.review_period_id AS review_period_id,
        rpr.performance_rating_id AS performance_rating_id,
        CAST(CASE
          WHEN upper(stg.mgr_goal_rating_numeric_value) = 'NOT APPLICABLE'
           OR upper(stg.mgr_goal_rating_numeric_value) = 'NOT RATED' THEN NULL
          ELSE CAST(stg.mgr_goal_rating_numeric_value as NUMERIC)
        END AS INT64) AS manager_goal_performance_rating_num,
        rpr2.performance_rating_id AS employee_goal_performance_rating_id,
        CAST(CASE
          WHEN upper(stg.emp_goal_rating_numeric_value) = 'NOT APPLICABLE'
           OR upper(stg.emp_goal_rating_numeric_value) = 'NOT RATED' THEN NULL
          ELSE CAST(stg.emp_goal_rating_numeric_value as NUMERIC)
        END AS INT64) AS employee_goal_performance_rating_num,
        CAST(coalesce(etp.lawson_company_num, 0) as INT64) AS lawson_company_num,
        trim(coalesce(etp.process_level_code, '00000')) AS process_level_code,
        stg.individual_goal_id AS source_system_key,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_perf_goals AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON trim(stg.individual_goal_id) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'EMPLOYEE_GOAL_DETAIL'
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON stg.employee_id = cast(emp.employee_num as string)
         AND emp.valid_to_date = DATETIME('9999-12-31 23:59:59')
         AND emp.lawson_company_num = CASE
           substr(stg.job_code, 1, 4)
          WHEN '' THEN 0
          ELSE CAST(substr(stg.job_code, 1, 4) as INT64)
        END
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_talent_profile AS etp 
              ON CASE 
                  WHEN TRIM(stg.employee_id) IS NOT NULL  
                        AND Substr(Trim(stg.employee_id),1,1) IN ('1','2','3','4','5','6','7','8','9')
                  THEN CASE TRIM(stg.employee_id)
                        WHEN '' THEN 0
                        ELSE CAST(TRIM(stg.employee_id) AS INT64)
                       END
                  ELSE 0 
                 END = etp.employee_num
              AND etp.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_category AS rpc 
        ON Upper(Trim(rpc.performance_category_desc)) = Upper(Trim(stg.goal_category))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_status AS rps 
        ON Upper(Trim(rps.performance_status_desc)) = Upper(Trim(stg.goal_status))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_status AS rps2 
        ON Upper(Trim(rps2.performance_status_desc)) = Upper(Trim(stg.goal_progress))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_plan AS rpn 
        ON Upper(Trim(rpn.performance_plan_desc)) = Upper(Trim(stg.plan_name))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_period AS rpp 
        ON Upper(Trim(rpp.review_period_desc)) = Upper(Trim(stg.review_period))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_rating AS rpr 
        ON Upper(Trim(rpr.performance_rating_desc)) = Upper(Trim(stg.mgr_goal_rating))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_performance_rating AS rpr2 
        ON Upper(Trim(rpr2.performance_rating_desc)) = Upper(Trim(stg.emp_goal_rating))
      WHERE emp.employee_sid <> 0
      QUALIFY row_number() OVER (PARTITION BY employee_goal_detail_sid ORDER BY employee_goal_detail_sid) = 1
  ;

  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_goal_detail AS tgt SET valid_to_date = (current_dt - INTERVAL 1 second), dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) FROM (
    SELECT
        employee_goal_detail_wrk.*
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_goal_detail_wrk
  ) AS wrk WHERE tgt.employee_goal_detail_sid = wrk.employee_goal_detail_sid
   AND (coalesce(tgt.employee_talent_profile_sid, 0) <> coalesce(wrk.employee_talent_profile_sid, 0)
   OR coalesce(tgt.employee_goal_year_num, 0) <> coalesce(wrk.employee_goal_year_num, 0)
   OR coalesce(tgt.employee_sid, 0) <> coalesce(wrk.employee_sid, 0)
   OR coalesce(tgt.employee_num, 0) <> coalesce(wrk.employee_num, 0)
   OR upper(coalesce(trim(tgt.goal_name), '')) <> upper(coalesce(trim(wrk.goal_name), ''))
   OR coalesce(tgt.goal_category_id, 0) <> coalesce(wrk.goal_category_id, 0)
   OR coalesce(tgt.goal_weight_pct, 0) <> coalesce(wrk.goal_weight_pct, 0)
   OR upper(coalesce(trim(tgt.expected_result_text), '')) <> upper(coalesce(trim(wrk.expected_result_text), ''))
   OR upper(coalesce(trim(tgt.goal_measurement_text), '')) <> upper(coalesce(trim(wrk.goal_measurement_text), ''))
   OR coalesce(tgt.goal_status_id, 0) <> coalesce(wrk.goal_status_id, 0)
   OR coalesce(tgt.goal_progress_status_id, 0) <> coalesce(wrk.goal_progress_status_id, 0)
   OR coalesce(tgt.goal_performance_plan_id, 0) <> coalesce(wrk.goal_performance_plan_id, 0)
   OR coalesce(tgt.goal_due_date, DATE '9999-01-01') <> coalesce(wrk.goal_due_date, DATE '9999-01-01')
   OR coalesce(tgt.user_defined_date, DATE '9999-01-01') <> coalesce(wrk.user_defined_date, DATE '9999-01-01')
   OR coalesce(tgt.review_year_num, 0) <> coalesce(wrk.review_year_num, 0)
   OR coalesce(tgt.review_period_end_date, DATE '9999-01-01') <> coalesce(wrk.review_period_end_date, DATE '9999-01-01')
   OR coalesce(tgt.review_period_start_date, DATE '9999-01-01') <> coalesce(wrk.review_period_start_date, DATE '9999-01-01')
   OR coalesce(tgt.review_period_id, 0) <> coalesce(wrk.review_period_id, 0)
   OR coalesce(tgt.manager_goal_performance_rating_id, 0) <> coalesce(wrk.manager_goal_performance_rating_id, 0)
   OR coalesce(tgt.manager_goal_performance_rating_num, 0) <> coalesce(wrk.manager_goal_performance_rating_num, 0)
   OR coalesce(tgt.employee_goal_performance_rating_id, 0) <> coalesce(wrk.employee_goal_performance_rating_id, 0)
   OR coalesce(tgt.employee_goal_performance_rating_num, 0) <> coalesce(wrk.employee_goal_performance_rating_num, 0)
   OR coalesce(tgt.lawson_company_num, 0) <> coalesce(wrk.lawson_company_num, 0)
   OR upper(coalesce(trim(tgt.process_level_code), '0')) <> upper(coalesce(trim(wrk.process_level_code), '0'))
   OR upper(coalesce(trim(tgt.source_system_key), '')) <> upper(coalesce(trim(wrk.source_system_key), '')))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT employee_goal_detail_sid, valid_from_date
      FROM {{ params.param_hr_core_dataset_name }}.employee_goal_detail
      GROUP BY employee_goal_detail_sid, valid_from_date
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:employee_goal_detail');
  ELSE
  COMMIT TRANSACTION;
  END IF;


BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_goal_detail (employee_goal_detail_sid, valid_from_date, employee_sid, employee_num, employee_talent_profile_sid, employee_goal_year_num, goal_name, goal_category_id, goal_weight_pct, expected_result_text, goal_measurement_text, goal_status_id, goal_progress_status_id, goal_performance_plan_id, goal_due_date, user_defined_date, review_year_num, review_period_end_date, review_period_start_date, review_period_id, manager_goal_performance_rating_id, manager_goal_performance_rating_num, employee_goal_performance_rating_id, employee_goal_performance_rating_num, lawson_company_num, process_level_code, valid_to_date, source_system_key, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.employee_goal_detail_sid,
        current_dt AS valid_from_date,
        wrk.employee_sid,
        wrk.employee_num,
        wrk.employee_talent_profile_sid,
        wrk.employee_goal_year_num,
        trim(wrk.goal_name),
        wrk.goal_category_id,
        wrk.goal_weight_pct,
        trim(wrk.expected_result_text),
        trim(wrk.goal_measurement_text),
        wrk.goal_status_id,
        wrk.goal_progress_status_id,
        wrk.goal_performance_plan_id,
        wrk.goal_due_date,
        wrk.user_defined_date,
        wrk.review_year_num,
        wrk.review_period_end_date,
        wrk.review_period_start_date,
        wrk.review_period_id,
        wrk.manager_goal_performance_rating_id,
        wrk.manager_goal_performance_rating_num,
        wrk.employee_goal_performance_rating_id,
        wrk.employee_goal_performance_rating_num,
        CAST(coalesce(wrk.lawson_company_num, 0) as INT64),
        trim(wrk.process_level_code),
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        trim(wrk.source_system_key),
        trim(wrk.source_system_code),
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_goal_detail_wrk AS wrk
      WHERE ( COALESCE(wrk.employee_goal_detail_sid,0),
              COALESCE(wrk.employee_talent_profile_sid,0),
              COALESCE(wrk.employee_sid,0), 
              COALESCE(wrk.employee_num,0), 
              COALESCE(wrk.employee_goal_year_num,0), 
              Upper(COALESCE(trim(wrk.goal_name),'')),
              COALESCE(wrk.goal_category_id,0), 
              COALESCE(wrk.goal_weight_pct,0), 
              Upper(COALESCE(trim(wrk.expected_result_text),'')), 
              Upper(COALESCE(trim(wrk.goal_measurement_text),'')), 
              COALESCE(wrk.goal_status_id,0), 
              COALESCE(wrk.goal_progress_status_id,0), 
              COALESCE(wrk.goal_performance_plan_id,0), 
              COALESCE(wrk.goal_due_date, DATE '9999-01-01'),
              COALESCE(wrk.user_defined_date, DATE '9999-01-01'), 
              COALESCE(wrk.review_year_num,0), 
              COALESCE(wrk.review_period_end_date, DATE '9999-01-01'), 
              COALESCE(wrk.review_period_start_date, DATE '9999-01-01'), 
              COALESCE(wrk.review_period_id,0), 
              COALESCE(wrk.manager_goal_performance_rating_id,0), 
              COALESCE(wrk.manager_goal_performance_rating_num,0), 
              COALESCE(wrk.employee_goal_performance_rating_id,0), 
              COALESCE(wrk.employee_goal_performance_rating_num,0), 
              COALESCE(wrk.lawson_company_num,0), 
              Upper(COALESCE(wrk.process_level_code,'')), 
              Upper(COALESCE(wrk.source_system_key,'')), 
              Upper(COALESCE(wrk.source_system_code,''))  
            ) 
            NOT IN 
            (
              SELECT AS STRUCT
                  COALESCE(tgt.employee_goal_detail_sid,0),
                  COALESCE(tgt.employee_talent_profile_sid,0),
                  COALESCE(tgt.employee_sid,0),
                  COALESCE(tgt.employee_num,0),
                  COALESCE(tgt.employee_goal_year_num,0),
                  Upper(COALESCE(trim(tgt.goal_name),'')), 
                  COALESCE(tgt.goal_category_id,0),
                  COALESCE(tgt.goal_weight_pct,0),
                  Upper(COALESCE(trim(tgt.expected_result_text),'')), 
                  Upper(COALESCE(trim(tgt.goal_measurement_text),'')), 
                  COALESCE(tgt.goal_status_id,0),
                  COALESCE(tgt.goal_progress_status_id,0),
                  COALESCE(tgt.goal_performance_plan_id,0),
                  COALESCE(tgt.goal_due_date, DATE '9999-01-01'), 
                  COALESCE(tgt.user_defined_date, DATE '9999-01-01'), 
                  COALESCE(tgt.review_year_num,0),
                  COALESCE(tgt.review_period_end_date, DATE '9999-01-01'), 
                  COALESCE(tgt.review_period_start_date, DATE '9999-01-01'), 
                  COALESCE(tgt.review_period_id,0),
                  COALESCE(tgt.manager_goal_performance_rating_id,0),
                  COALESCE(tgt.manager_goal_performance_rating_num,0),
                  COALESCE(tgt.employee_goal_performance_rating_id,0),
                  COALESCE(tgt.employee_goal_performance_rating_num,0),
                  COALESCE(tgt.lawson_company_num, 0),
                  Upper(COALESCE(tgt.process_level_code,'')), 
                  Upper(COALESCE(tgt.source_system_key,'')), 
                  Upper(COALESCE(tgt.source_system_code,''))
              FROM
                {{ params.param_hr_base_views_dataset_name }}.employee_goal_detail AS tgt
              WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
            );
      
          SET DUP_COUNT = (
                            SELECT COUNT(*) 
                            FROM (
                                    SELECT employee_goal_detail_sid, valid_from_date
                                    FROM {{ params.param_hr_core_dataset_name }}.employee_goal_detail
                                    GROUP BY employee_goal_detail_sid, valid_from_date
                                    HAVING COUNT(*) > 1
                                )
                          );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.employee_goal_detail');
  ELSE
  COMMIT TRANSACTION;
  END IF;

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_goal_detail AS tgt SET valid_to_date = (current_dt - INTERVAL 1 second), dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND tgt.employee_goal_detail_sid NOT IN(
    SELECT DISTINCT
        wrk.employee_goal_detail_sid
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_goal_detail_wrk AS wrk
  );
END;
