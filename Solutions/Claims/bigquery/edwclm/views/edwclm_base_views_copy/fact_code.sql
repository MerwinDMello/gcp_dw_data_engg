CREATE OR REPLACE VIEW {{ params.param_clm_mirrored_base_views_dataset_name }}.fact_code
AS SELECT
		fact_code.claim_id,
		fact_code.code_seq_num,
		fact_code.code_type_id,
		fact_code.code_value,
		fact_code.code_amt,
		fact_code.code_from_dt,
		fact_code.code_thru_dt,
		fact_code.code_poa_ind,
		fact_code.dw_last_update_date_time,
		fact_code.source_system_code
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_clm_mirrored_core_dataset_name }}.fact_code
;
