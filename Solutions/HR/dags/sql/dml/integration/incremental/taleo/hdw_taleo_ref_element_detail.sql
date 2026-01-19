	BEGIN
	  DECLARE dup_count INT64;

	  BEGIN TRANSACTION;
	/*  Load Target Table with Staging Data */

	  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_element_detail AS tgt USING (
		SELECT
			udse.number AS element_detail_id,
			udse.userdefinedselection_number AS element_definition_selection_id,
			cast(trim(udse.active) as int64) AS active_sw,
			trim(udse.code) AS element_code,
			trim(udse.description) AS element_desc,
			cast(trim(udse.sequence) as int64) AS element_seq_num,
			cast(trim(udse.complete) as int64) AS complete_sw,
			udse.effectivefrom AS eff_from_date_time,
			udse.effectiveuntil AS eff_to_date_time,
			udse.lastmodifieddate AS last_modified_date_time,
			udse.source_system_code,
			DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
		  FROM
			{{ params.param_hr_stage_dataset_name }}.taleo_udselement AS udse
		  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
	  ) AS stg
	  ON tgt.element_detail_id = stg.element_detail_id
	   AND tgt.element_definition_selection_id = stg.element_definition_selection_id
		 WHEN MATCHED THEN UPDATE SET active_sw = stg.active_sw, element_code = stg.element_code, element_desc = stg.element_desc, element_seq_num = stg.element_seq_num, complete_sw = stg.complete_sw, eff_from_date_time = stg.eff_from_date_time, eff_to_date_time = stg.eff_to_date_time, last_modified_date_time = stg.last_modified_date_time, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
		 WHEN NOT MATCHED BY TARGET THEN INSERT (element_detail_id, element_definition_selection_id, active_sw, element_code, element_desc, element_seq_num, complete_sw, eff_from_date_time, eff_to_date_time, last_modified_date_time, source_system_code, dw_last_update_date_time) VALUES (stg.element_detail_id, stg.element_definition_selection_id, stg.active_sw, stg.element_code, stg.element_desc, stg.element_seq_num, stg.complete_sw, stg.eff_from_date_time, stg.eff_to_date_time, stg.last_modified_date_time, stg.source_system_code, stg.dw_last_update_date_time)
	  ;


		/* Test Unique Index constraint set in Terdata */
		SET dup_count = ( 
			select count(*)
			from (
			select
				element_detail_id ,element_definition_selection_id
			from {{ params.param_hr_core_dataset_name }}.ref_element_detail
			group by element_detail_id ,element_definition_selection_id
			having count(*) > 1
			)
		);
		IF dup_count <> 0 THEN
		  ROLLBACK TRANSACTION;
		  RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_element_detail');
		ELSE
		  COMMIT TRANSACTION;
		END IF;
	END;