-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/rh_837_claim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim
   OPTIONS(description='The Fact_837_Claim table will hold specific claim data elements. This will vary from admission and discharge dates, patient identifiers, DRG, etc')
  AS SELECT
      rh_837_claim.claim_id,
      rh_837_claim.patient_dw_id,
      rh_837_claim.company_code,
      rh_837_claim.coid,
      rh_837_claim.unit_num,
      rh_837_claim.pat_acct_num,
      rh_837_claim.medical_record_num,
      rh_837_claim.iplan_id,
      rh_837_claim.iplan_insurance_order_num,
      rh_837_claim.financial_class_code,
      rh_837_claim.patient_type_code,
      rh_837_claim.stmt_cover_from_date,
      rh_837_claim.stmt_cover_to_date,
      rh_837_claim.admission_date,
      rh_837_claim.admission_hour,
      rh_837_claim.admission_type_code,
      rh_837_claim.admission_source_code,
      rh_837_claim.discharge_hour,
      rh_837_claim.discharge_status_code,
      rh_837_claim.bill_date,
      rh_837_claim.bill_type_code,
      rh_837_claim.diag_code_admit,
      rh_837_claim.drg_code,
      rh_837_claim.total_charge_amt,
      rh_837_claim.accident_state_code,
      rh_837_claim.claim_remark_text,
      rh_837_claim.edi_837_claim_type_code,
      rh_837_claim.claim_destination_method_code,
      rh_837_claim.facility_prefix_code,
      rh_837_claim.national_provider_id,
      rh_837_claim.federal_tax_num,
      rh_837_claim.provider_taxonomy_code,
      rh_837_claim.service_code,
      rh_837_claim.pas_coid,
      rh_837_claim.service_center_type_code,
      rh_837_claim.source_system_code,
      rh_837_claim.dw_last_update_date_time
    FROM
      {{ params.param_auth_base_views_dataset_name }}.rh_837_claim
  ;
