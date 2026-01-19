BEGIN
DECLARE DUP_COUNT INT64;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk (performance_rating_id, performance_rating_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(performance_rating_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_performance_rating
        ) + CAST(row_number() OVER (ORDER BY y.performance_rating_desc) as int64) AS performance_rating_id,
        y.performance_rating_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              TRIM(stg.performance_rating_desc) AS performance_rating_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(employee_info.calibrate_overall_perf_rating) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info
                  WHERE TRIM(employee_info.calibrate_overall_perf_rating) IS NOT NULL
                   AND TRIM(employee_info.calibrate_overall_perf_rating) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_info_0.overall_performance_rating) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_0
                  WHERE TRIM(employee_info_0.overall_performance_rating) IS NOT NULL
                   AND TRIM(employee_info_0.overall_performance_rating) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_perf_goals.emp_goal_rating) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_perf_goals
                  WHERE TRIM(employee_perf_goals.emp_goal_rating) IS NOT NULL
                   AND TRIM(employee_perf_goals.emp_goal_rating) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_perf_goals_0.mgr_goal_rating) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_perf_goals AS employee_perf_goals_0
                  WHERE TRIM(employee_perf_goals_0.mgr_goal_rating) IS NOT NULL
                   AND TRIM(employee_perf_goals_0.mgr_goal_rating) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(competency_ratings_report.employee_rating_scale_value) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report
                  WHERE TRIM(competency_ratings_report.employee_rating_scale_value) IS NOT NULL
                   AND TRIM(competency_ratings_report.employee_rating_scale_value) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(competency_ratings_report_0.manager_rating_scale_value) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.competency_ratings_report AS competency_ratings_report_0
                  WHERE TRIM(competency_ratings_report_0.manager_rating_scale_value) IS NOT NULL
                   AND TRIM(competency_ratings_report_0.manager_rating_scale_value) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report.emp_perf_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report
                  WHERE TRIM(performance_ratings_report.emp_perf_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report.emp_perf_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report_0.perf_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS performance_ratings_report_0
                  WHERE TRIM(performance_ratings_report_0.perf_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report_0.perf_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report_1.emp_smry_comp_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS performance_ratings_report_1
                  WHERE TRIM(performance_ratings_report_1.emp_smry_comp_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report_1.emp_smry_comp_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report_2.sumry_comp_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS performance_ratings_report_2
                  WHERE TRIM(performance_ratings_report_2.sumry_comp_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report_2.sumry_comp_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report_3.emp_smry_goal_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS performance_ratings_report_3
                  WHERE TRIM(performance_ratings_report_3.emp_smry_goal_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report_3.emp_smry_goal_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(performance_ratings_report_4.smry_goal_rat_scale_val) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.performance_ratings_report AS performance_ratings_report_4
                  WHERE TRIM(performance_ratings_report_4.smry_goal_rat_scale_val) IS NOT NULL
                   AND TRIM(performance_ratings_report_4.smry_goal_rat_scale_val) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(map_tms_box.ovrall_calibrated_perf_rvw) AS performance_rating_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.map_tms_box
                  WHERE TRIM(map_tms_box.ovrall_calibrated_perf_rvw) IS NOT NULL
                   AND TRIM(map_tms_box.ovrall_calibrated_perf_rvw) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.performance_rating_desc,''))) = UPPER(TRIM(COALESCE(tgt.performance_rating_desc,'')))
            WHERE tgt.performance_rating_desc IS NULL
        ) AS y
  ;

DELETE FROM {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk WHERE UPPER(ref_performance_rating_wrk.performance_rating_desc) = '----------';

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_performance_rating (performance_rating_id, performance_rating_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.performance_rating_id) as int64) + (
          SELECT
              COALESCE(max(performance_rating_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_performance_rating
        ) AS performance_rating_id,
        TRIM(wrk.performance_rating_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk AS wrk
      WHERE UPPER(TRIM(COALESCE(wrk.performance_rating_desc,''))) NOT IN(
        SELECT
            UPPER(TRIM(COALESCE(performance_rating_desc,'')))
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_performance_rating
      )
  ;
    SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT performance_rating_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_performance_rating
      GROUP BY performance_rating_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION;
    RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_performance_rating');
  ELSE
    COMMIT TRANSACTION;
  END IF;

END;
