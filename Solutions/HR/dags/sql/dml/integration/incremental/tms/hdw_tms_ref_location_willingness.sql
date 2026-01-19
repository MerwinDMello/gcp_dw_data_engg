BEGIN
DECLARE DUP_COUNT INT64;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_location_willingness_wrk;

  DELETE FROM {{ params.param_hr_stage_dataset_name }}.ref_location_willingness_wrk WHERE TRUE;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.ref_location_willingness_wrk (location_willingness_id, location_willingness_desc, source_system_code, dw_last_update_date_time)
    SELECT
        (
          SELECT
              COALESCE(max(location_willingness_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_location_willingness
        ) + CAST(row_number() OVER (ORDER BY y.location_willingness_desc) as int64) AS location_willingness_id,
        y.location_willingness_desc,
        'M' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              TRIM(stg.location_willingness_desc) AS location_willingness_desc
            FROM
              (
                SELECT DISTINCT
                    TRIM(employee_info.willing_to_relocate) AS location_willingness_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info
                  WHERE TRIM(employee_info.willing_to_relocate) IS NOT NULL
                   AND TRIM(employee_info.willing_to_relocate) <> ''
                UNION DISTINCT
                SELECT DISTINCT
                    TRIM(employee_info_0.willing_to_travel) AS location_willingness_desc
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.employee_info AS employee_info_0
                  WHERE TRIM(employee_info_0.willing_to_travel) IS NOT NULL
                   AND TRIM(employee_info_0.willing_to_travel) <> ''
              ) AS stg
              LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_location_willingness_wrk AS tgt 
              ON UPPER(TRIM(COALESCE(stg.location_willingness_desc,''))) = UPPER(TRIM(COALESCE(tgt.location_willingness_desc,'')))
            WHERE tgt.location_willingness_desc IS NULL
        ) AS y
  ;

  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_location_willingness (location_willingness_id, location_willingness_desc, source_system_code, dw_last_update_date_time)
    SELECT
        CAST(row_number() OVER (ORDER BY wrk.location_willingness_id) as int64) + (
          SELECT
              COALESCE(max(location_willingness_id), CAST(0 as int64))
            FROM
              {{ params.param_hr_core_dataset_name }}.ref_location_willingness
        ) AS location_willingness_id,
        TRIM(wrk.location_willingness_desc),
        wrk.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_location_willingness_wrk AS wrk
      WHERE UPPER(TRIM(COALESCE(wrk.location_willingness_desc,''))) NOT IN(
        SELECT
             UPPER(TRIM(COALESCE(location_willingness_desc,''))) AS location_willingness_desc
          FROM
            {{ params.param_hr_core_dataset_name }}.ref_location_willingness AS ref_location_willingness_0
      )
  ;
  SET DUP_COUNT = (
    SELECT COUNT(*) FROM(
      SELECT location_willingness_id
      FROM {{ params.param_hr_core_dataset_name }}.ref_location_willingness
      GROUP BY location_willingness_id
      HAVING COUNT(*) > 1
    )
  );

  IF DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION;
    RAISE USING MESSAGE = CONCAT(' Duplicates not allowed in table: {{ params.param_hr_core_dataset_name }}.ref_location_willingness ');
  ELSE
    COMMIT TRANSACTION;
  END IF;
  
  END;
