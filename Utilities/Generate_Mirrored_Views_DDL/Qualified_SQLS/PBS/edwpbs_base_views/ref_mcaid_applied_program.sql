-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_mcaid_applied_program.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_mcaid_applied_program
   OPTIONS(description='Reference details of Applied Programs for Medicaid eligibility accounts.')
  AS SELECT
      ref_mcaid_applied_program.program_code,
      ref_mcaid_applied_program.program_desc,
      ref_mcaid_applied_program.source_system_code,
      ref_mcaid_applied_program.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_mcaid_applied_program
  ;
