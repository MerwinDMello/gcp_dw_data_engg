BEGIN
  DECLARE dup_count INT64;

  BEGIN TRANSACTION;
/*  Load Work Table with working Data */

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_element_definition_type AS tgt USING (
    SELECT
        udf.number AS element_detail_definition_type_id,
        trim(udf.description) AS element_detail_definition_type_desc,
        udf.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_udftype AS udf
      GROUP BY 1, 2, 3, 4
  ) AS stg
  ON tgt.element_detail_definition_type_id = stg.element_detail_definition_type_id
     WHEN MATCHED THEN UPDATE SET element_detail_definition_type_desc = stg.element_detail_definition_type_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (element_detail_definition_type_id, element_detail_definition_type_desc, source_system_code, dw_last_update_date_time) VALUES (stg.element_detail_definition_type_id, stg.element_detail_definition_type_desc, stg.source_system_code, stg.dw_last_update_date_time)
  ;


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            element_detail_definition_type_id
        from {{ params.param_hr_core_dataset_name }}.ref_element_definition_type
        group by element_detail_definition_type_id
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_element_definition_type');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;