BEGIN
  DECLARE dup_count INT64;

  BEGIN TRANSACTION;
/*  Load Target Table with Staging Data */

  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_element_definition AS tgt USING (
    SELECT
        trim(udfd.entity) AS element_detail_entity_text,
        trim(udfd.id) AS element_detail_type_text,
        trim(udfd.description) AS element_detail_definition_desc,
        udfd.type_number AS element_detail_definition_type_id,
        udfd.userdefinedselection_number AS element_definition_selection_id,
        udfd.source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_udfdefinition AS udfd
      QUALIFY row_number() OVER (PARTITION BY udfd.entity, udfd.id ORDER BY udfd.entity, udfd.id) = 1
  ) AS stg
  ON tgt.element_detail_entity_text = stg.element_detail_entity_text
   AND tgt.element_detail_type_text = stg.element_detail_type_text
     WHEN MATCHED THEN UPDATE SET element_detail_definition_desc = stg.element_detail_definition_desc, element_detail_definition_type_id = stg.element_detail_definition_type_id, element_definition_selection_id = stg.element_definition_selection_id, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (element_detail_entity_text, element_detail_type_text, element_detail_definition_desc, element_detail_definition_type_id, element_definition_selection_id, source_system_code, dw_last_update_date_time) VALUES (stg.element_detail_entity_text, stg.element_detail_type_text, stg.element_detail_definition_desc, stg.element_detail_definition_type_id, stg.element_definition_selection_id, stg.source_system_code, stg.dw_last_update_date_time)
  ;


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            element_detail_entity_text ,element_detail_type_text
        from {{ params.param_hr_core_dataset_name }}.ref_element_definition
        group by element_detail_entity_text ,element_detail_type_text
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_element_definition');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;