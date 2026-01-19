BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_hr_code AS tgt USING (
    SELECT
        stg1.hr_code,
        stg1.hr_type_code,
        stg1.hr_code_desc,
        stg1.active_ind,
        stg1.source_system_code,
        stg1.dw_last_update_date_time
      FROM
        (
          SELECT
              trim(code) AS hr_code,
              trim(type) AS hr_type_code,
              trim(description) AS hr_code_desc,
              trim(active_flag) AS active_ind,
              'L' AS source_system_code,
              datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
            FROM
              {{ params.param_hr_stage_dataset_name }}.pcodes
            GROUP BY 1, 2, 3, 4
        ) AS stg1
      QUALIFY row_number() OVER (PARTITION BY stg1.hr_code, stg1.hr_type_code ORDER BY stg1.active_ind, upper(stg1.source_system_code) DESC) = 1
  ) AS stg
  ON stg.hr_code = tgt.hr_code
   AND stg.hr_type_code = tgt.hr_type_code
     WHEN MATCHED THEN UPDATE SET hr_code_desc = stg.hr_code_desc, active_ind = stg.active_ind, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (hr_code, hr_type_code, hr_code_desc, active_ind, source_system_code, dw_last_update_date_time) VALUES (stg.hr_code, stg.hr_type_code, stg.hr_code_desc, stg.active_ind, stg.source_system_code, stg.dw_last_update_date_time)
  ;
  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select HR_Code ,HR_Type_Code
        from {{ params.param_hr_core_dataset_name }}.ref_hr_code
        group by HR_Code ,HR_Type_Code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_hr_code');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;