BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);


BEGIN TRANSACTION;
  
/*  Ref Table Load   */
  DELETE FROM {{ params.param_hr_core_dataset_name }}.ref_lawson_parameter WHERE 1=1;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.ref_lawson_parameter (parameter_code_1, parameter_code_2, parameter_code_3, parameter_group_code, detail_value_text, source_system_code, dw_last_update_date_time)
    SELECT
        a.lvl_1_key,
        a.lvl_2_key,
        a.lvl_3_key,
        a.dtl_key,
        a.dtl_value,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        (
          SELECT
              trim(zxgnxrefd.lvl_1_key) as lvl_1_key,
              trim(zxgnxrefd.lvl_2_key) as lvl_2_key,
              trim(zxgnxrefd.lvl_3_key) as lvl_3_key,
              trim(zxgnxrefd.dtl_key) as dtl_key,
              trim(zxgnxrefd.dtl_value) as dtl_value
            FROM
              {{ params.param_hr_stage_dataset_name }}.zxgnxrefd
            GROUP BY 1, 2, 3, 4, 5
        ) AS a
  ;

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Parameter_Code_1 ,Parameter_Code_2 ,Parameter_Code_3 ,Parameter_Group_Code
        from {{ params.param_hr_core_dataset_name }}.ref_lawson_parameter
        group by Parameter_Code_1 ,Parameter_Code_2 ,Parameter_Code_3 ,Parameter_Group_Code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_lawson_parameter');
    ELSE
      COMMIT TRANSACTION;
    END IF;

    END;
