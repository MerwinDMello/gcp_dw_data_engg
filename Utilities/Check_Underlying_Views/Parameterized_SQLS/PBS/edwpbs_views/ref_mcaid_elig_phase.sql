-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_mcaid_elig_phase.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_mcaid_elig_phase
   OPTIONS(description='Reference table to describe Medicaid Eligibility Phases')
  AS SELECT
      a.mcaid_elig_phase_id,
      a.mcaid_elig_phase_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_mcaid_elig_phase AS a
  ;
