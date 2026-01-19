-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_charge_rev_smry.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_charge_rev_smry
   OPTIONS(description='The RH_837_Charge_Rev_Smry table will be set up to store the summarized charges at the Revenue Code level. This data ties to UB Form Locators 42-48.')
  AS SELECT
      rh_837_charge_rev_smry.claim_id,
      rh_837_charge_rev_smry.charge_smry_seq_num,
      rh_837_charge_rev_smry.patient_dw_id,
      rh_837_charge_rev_smry.company_code,
      rh_837_charge_rev_smry.coid,
      rh_837_charge_rev_smry.pat_acct_num,
      rh_837_charge_rev_smry.charge_revenue_code,
      rh_837_charge_rev_smry.charge_service_date,
      rh_837_charge_rev_smry.charge_service_factor_qty,
      rh_837_charge_rev_smry.charge_total_amt,
      rh_837_charge_rev_smry.charge_non_covered_amt,
      rh_837_charge_rev_smry.hcpcs_procedure_code,
      rh_837_charge_rev_smry.hcpcs_procedure_modifier_code1,
      rh_837_charge_rev_smry.hcpcs_procedure_modifier_code2,
      rh_837_charge_rev_smry.hcpcs_procedure_modifier_code3,
      rh_837_charge_rev_smry.hcpcs_procedure_modifier_code4,
      rh_837_charge_rev_smry.ndc_code,
      rh_837_charge_rev_smry.ndc_drug_qty,
      rh_837_charge_rev_smry.ndc_drug_uom_code,
      rh_837_charge_rev_smry.source_system_code,
      rh_837_charge_rev_smry.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_charge_rev_smry
  ;
