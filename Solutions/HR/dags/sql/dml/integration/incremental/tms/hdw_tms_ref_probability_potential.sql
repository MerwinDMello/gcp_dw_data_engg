BEGIN
DECLARE DUP_COUNT INT64;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_probability_potential_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_probability_potential_wrk (probability_potential_id, probability_potential_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(probability_potential_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_probability_potential
        ) + CAST(row_number() OVER (ORDER BY y.probability_potential_desc) as int64) AS probability_potential_id,
        y.probability_potential_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              TRIM(stg.probability_potential_desc) AS probability_potential_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(employee_info.potential) AS probability_potential_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info
                  WHERE TRIM(employee_info.potential) IS NOT NULL
                   AND TRIM(employee_info.potential) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_info_0.employee_promote_interest) AS probability_potential_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_0
                  WHERE TRIM(employee_info_0.employee_promote_interest) IS NOT NULL
                   AND TRIM(employee_info_0.employee_promote_interest) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_info_1.flight_risk) AS probability_potential_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_1
                  WHERE TRIM(employee_info_1.flight_risk) IS NOT NULL
                   AND TRIM(employee_info_1.flight_risk) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(development_activities_report.priority) AS probability_potential_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.development_activities_report
                  WHERE TRIM(development_activities_report.priority) IS NOT NULL
                   AND TRIM(development_activities_report.priority) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(map_tms_box.tms_box_potential) AS probability_potential_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.map_tms_box
                  WHERE TRIM(map_tms_box.tms_box_potential) IS NOT NULL
                   AND TRIM(map_tms_box.tms_box_potential) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_probability_potential_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.probability_potential_desc,''))) = UPPER(TRIM(COALESCE(tgt.probability_potential_desc,'')))
            WHERE tgt.probability_potential_desc IS NULL
        ) AS y
  ;  

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_probability_potential (probability_potential_id, probability_potential_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.probability_potential_id) as int64) + (
          SELECT
              COALESCE(max(probability_potential_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_probability_potential
        ) AS probability_potential_id,
        TRIM(wrk.probability_potential_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_probability_potential_wrk AS wrk
      WHERE UPPER(TRIM(COALESCE(wrk.probability_potential_desc,''))) NOT IN(
        SELECT
            UPPER(TRIM(COALESCE(probability_potential_desc,'')))
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_probability_potential
      )
  ;
      SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT probability_potential_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_probability_potential
      GROUP BY probability_potential_id
      HAVING COUNT(*) > 1
    )
  );
  
  IF DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION;
    RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_probability_potential ');
  ELSE
    COMMIT TRANSACTION;
  END IF;

END;
