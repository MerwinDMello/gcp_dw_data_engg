BEGIN
DECLARE DUP_COUNT INT64;

BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_percentage_range (percentage_range_sid, percentage_range_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              coalesce(max(percentage_range_sid), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_percentage_range
        ) + CAST(row_number() OVER (ORDER BY y.percentage_range_desc) as int64) AS percentage_range_sid,
        y.percentage_range_desc,
        'M' AS source_system_code,
        timestamp_trunc(current_datetime("US/Central"), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              stg.percentage_range_desc
            FROM
              (
                SELECT DISTINCT
                    trim(employee_info.willing_travel_percentage) AS percentage_range_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info
                  WHERE trim(employee_info.willing_travel_percentage) IS NOT NULL
                   AND trim(employee_info.willing_travel_percentage) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_percentage_range AS tgt ON stg.percentage_range_desc = tgt.percentage_range_desc
            WHERE tgt.percentage_range_desc IS NULL
        ) AS y
  ;
  
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT percentage_range_sid
      FROM {{ params.param_hr_core_dataset_name }}.ref_percentage_range
      GROUP BY percentage_range_sid
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table:{{ params.param_hr_core_dataset_name }}.ref_percentage_range ');
  ELSE 
  COMMIT TRANSACTION;
  END IF;
END;