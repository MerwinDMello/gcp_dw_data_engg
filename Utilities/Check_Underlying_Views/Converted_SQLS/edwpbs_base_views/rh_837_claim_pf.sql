-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rh_837_claim_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rh_837_claim_pf AS SELECT
    a.claim_id,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.medical_record_num,
    a.iplan_id,
    a.iplan_insurance_order_num,
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
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
    ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
    a.accident_state_code,
    a.claim_remark_text,
    a.edi_837_claim_type_code,
    a.claim_destination_method_code,
    a.facility_prefix_code,
    ROUND(a.national_provider_id, 0, 'ROUND_HALF_EVEN') AS national_provider_id,
    a.federal_tax_num,
    a.provider_taxonomy_code,
    a.service_code,
    a.pas_coid,
    a.service_center_type_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.rh_837_claim AS a
;
