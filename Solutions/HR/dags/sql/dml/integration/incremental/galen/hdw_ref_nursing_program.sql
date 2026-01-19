BEGIN
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_nursing_program AS tgt USING (
    SELECT
        nursing_program_id,
        program_name,
        program_type_code,
        program_degree_text,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_nursing_program_wrk
  ) AS src
  ON tgt.nursing_program_id = src.nursing_program_id
     WHEN MATCHED THEN UPDATE SET program_name = src.program_name, program_type_code = src.program_type_code, program_degree_text = src.program_degree_text, source_system_code = src.source_system_code, dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (nursing_program_id, program_name, program_type_code, program_degree_text, source_system_code, dw_last_update_date_time) VALUES (src.nursing_program_id, src.program_name, src.program_type_code, src.program_degree_text, src.source_system_code, src.dw_last_update_date_time)
  ;
END ;
