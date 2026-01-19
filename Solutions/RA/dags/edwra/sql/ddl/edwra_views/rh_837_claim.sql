-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/rh_837_claim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.rh_837_claim
   OPTIONS(description='The Fact_837_Claim table will hold specific claim data elements. This will vary from admission and discharge dates, patient identifiers, DRG, etc')
  AS SELECT
      a.claim_id,
      a.patient_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      a.pat_acct_num,
      a.medical_record_num,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.financial_class_code,
      a.patient_type_code,
      a.stmt_cover_from_date,
      a.stmt_cover_to_date,
      a.admission_date,
      a.admission_hour,
      a.admission_type_code,
      a.admission_source_code,
      a.discharge_hour,
      a.discharge_status_code,
      a.bill_date,
      a.bill_type_code,
      a.diag_code_admit,
      a.drg_code,
      a.total_charge_amt,
      a.accident_state_code,
      a.claim_remark_text,
      a.edi_837_claim_type_code,
      a.claim_destination_method_code,
      a.facility_prefix_code,
      a.national_provider_id,
      a.federal_tax_num,
      a.provider_taxonomy_code,
      a.service_code,
      a.pas_coid,
      a.service_center_type_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.rh_837_claim AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
