-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_eligibility_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_mcaid_eligibility_status
   OPTIONS(description='Reference table with status details of Medicaid eligibility accounts.')
  AS SELECT
      ref_mcaid_eligibility_status.status_code,
      ref_mcaid_eligibility_status.status_desc,
      ref_mcaid_eligibility_status.source_system_code,
      ref_mcaid_eligibility_status.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.ref_mcaid_eligibility_status
  ;
