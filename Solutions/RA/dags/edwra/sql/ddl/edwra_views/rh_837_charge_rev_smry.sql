-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/rh_837_charge_rev_smry.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.rh_837_charge_rev_smry
   OPTIONS(description='The RH_837_Charge_Rev_Smry table will be set up to store the summarized charges at the Revenue Code level. This data ties to UB Form Locators 42-48.')
  AS SELECT
      a.claim_id,
      a.charge_smry_seq_num,
      a.patient_dw_id,
      a.company_code,
      a.coid,
      a.pat_acct_num,
      a.charge_revenue_code,
      a.charge_service_date,
      a.charge_service_factor_qty,
      a.charge_total_amt,
      a.charge_non_covered_amt,
      a.hcpcs_procedure_code,
      a.hcpcs_procedure_modifier_code1,
      a.hcpcs_procedure_modifier_code2,
      a.hcpcs_procedure_modifier_code3,
      a.hcpcs_procedure_modifier_code4,
      a.ndc_code,
      a.ndc_drug_qty,
      a.ndc_drug_uom_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_charge_rev_smry AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
