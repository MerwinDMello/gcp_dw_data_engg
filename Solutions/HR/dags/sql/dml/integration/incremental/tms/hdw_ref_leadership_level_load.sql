BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_leadership_level (leadership_level_sid, leadership_level_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(leadership_level_sid), CAST(0 as INT64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_leadership_level
        ) + CAST(row_number() OVER (ORDER BY y.leadership_level_desc) as INT64) AS flight_risk_sid,
        y.leadership_level_desc,
        'M' AS source_system_code,
        timestamp_trunc(current_datetime("US/Central"), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              stg.leadership_level_desc
            FROM
              (
                SELECT
                    trim(employee_info.future_role2_leadership_level) AS leadership_level_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info
                  WHERE trim(employee_info.future_role2_leadership_level) IS NOT NULL
                   AND trim(employee_info.future_role2_leadership_level) <> ''
                UNION ALL
                SELECT
                    trim(employee_info_0.future_role1_leadership_level) AS leadership_level_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_0
                  WHERE trim(employee_info_0.future_role1_leadership_level) IS NOT NULL
                   AND trim(employee_info_0.future_role1_leadership_level) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_leadership_level AS tgt ON stg.leadership_level_desc = tgt.leadership_level_desc
            WHERE tgt.leadership_level_desc IS NULL
        ) AS y
  ;
  
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT leadership_level_sid
      FROM {{ params.param_hr_core_dataset_name }}.ref_leadership_level
      GROUP BY leadership_level_sid
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.ref_leadership_level ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;