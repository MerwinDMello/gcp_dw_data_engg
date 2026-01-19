CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.fact_claim_physician
AS SELECT
		fact_claim_physician.claim_id,
		fact_claim_physician.phys_type_code,
		fact_claim_physician.phys_qual_code,
		fact_claim_physician.phys_code,
		fact_claim_physician.phys_last_name,
		fact_claim_physician.phys_first_name,
		fact_claim_physician.phys_taxonomy_code,
		fact_claim_physician.dw_last_update_date_time,
		fact_claim_physician.source_system_code
  FROM
    {{ params.param_clm_core_dataset_name }}.fact_claim_physician
;
