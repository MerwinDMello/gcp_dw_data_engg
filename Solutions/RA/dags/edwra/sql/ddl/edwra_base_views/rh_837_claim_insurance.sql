-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_claim_insurance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim_insurance
   OPTIONS(description='The RH_837_Claim_Insurance table will hold specific data elements pertaining to the Insurance or Payor.')
  AS SELECT
      rh_837_claim_insurance.claim_id,
      rh_837_claim_insurance.iplan_insurance_order_num,
      rh_837_claim_insurance.patient_dw_id,
      rh_837_claim_insurance.company_code,
      rh_837_claim_insurance.coid,
      rh_837_claim_insurance.pat_acct_num,
      rh_837_claim_insurance.iplan_id,
      rh_837_claim_insurance.payor_name,
      rh_837_claim_insurance.health_plan_id,
      rh_837_claim_insurance.other_provider_id,
      rh_837_claim_insurance.hic_claim_num,
      rh_837_claim_insurance.insured_name,
      rh_837_claim_insurance.pat_relationship_to_ins_code,
      rh_837_claim_insurance.insured_group_name,
      rh_837_claim_insurance.insured_group_num,
      rh_837_claim_insurance.treatment_auth_code,
      rh_837_claim_insurance.signed_pat_rel_on_file_ind,
      rh_837_claim_insurance.signed_assn_benf_on_file_ind,
      rh_837_claim_insurance.prior_pmt_smry_amt,
      rh_837_claim_insurance.estimated_rmd_due_amt,
      rh_837_claim_insurance.document_control_num,
      rh_837_claim_insurance.source_system_code,
      rh_837_claim_insurance.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_claim_insurance
  ;
