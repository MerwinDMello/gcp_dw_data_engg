	BEGIN
	  DECLARE dup_count INT64;

	  BEGIN TRANSACTION;
	/*  Load Target Table with Staging Data */

	 MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_element_definition_selection AS tgt USING (
		SELECT
			user_def.number AS element_definition_selection_id,
			trim(user_def.code) AS element_definition_selection_code,
			trim(user_def.name) AS element_definition_selection_name,
			user_def.source_system_code,
			timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
		  FROM
			{{ params.param_hr_stage_dataset_name }}.taleo_userdefinedselection AS user_def
		  GROUP BY 1, 2, 3, 4, 5
	  ) AS stg
	  ON tgt.element_definition_selection_id = stg.element_definition_selection_id
		 WHEN MATCHED THEN UPDATE SET element_definition_selection_code = stg.element_definition_selection_code, element_definition_selection_name = stg.element_definition_selection_name, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
		 WHEN NOT MATCHED BY TARGET THEN INSERT (element_definition_selection_id, element_definition_selection_code, element_definition_selection_name, source_system_code, dw_last_update_date_time) VALUES (stg.element_definition_selection_id, stg.element_definition_selection_code, stg.element_definition_selection_name, stg.source_system_code, stg.dw_last_update_date_time)
	  ;


		/* Test Unique Index constraint set in Terdata */
		SET dup_count = ( 
			select count(*)
			from (
			select
				element_definition_selection_id
			from {{ params.param_hr_core_dataset_name }}.ref_element_definition_selection
			group by element_definition_selection_id
			having count(*) > 1
			)
		);
		IF dup_count <> 0 THEN
		  ROLLBACK TRANSACTION;
		  RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_element_definition_selection');
		ELSE
		  COMMIT TRANSACTION;
		END IF;
	END;