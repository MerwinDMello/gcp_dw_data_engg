-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_elig_phase.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_mcaid_elig_phase
   OPTIONS(description='Reference table to describe Medicaid Eligibility Phases')
  AS SELECT
      ref_mcaid_elig_phase.mcaid_elig_phase_id,
      ref_mcaid_elig_phase.mcaid_elig_phase_desc,
      ref_mcaid_elig_phase.source_system_code,
      ref_mcaid_elig_phase.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_mcaid_elig_phase
  ;
