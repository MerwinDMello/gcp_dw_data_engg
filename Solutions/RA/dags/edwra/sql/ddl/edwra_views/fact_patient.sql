-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/fact_patient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.fact_patient AS SELECT
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.sub_unit_num,
    a.admission_patient_type_code,
    a.pat_acct_num,
    a.patient_type_code_pos1,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.financial_class_code,
    a.account_status_code,
    a.admission_source_code,
    a.admission_type_code,
    a.drg_code_payor,
    a.drg_code_hcfa,
    a.drg_hcfa_icd10_code,
    a.drg_desc_hcfa,
    a.drg_payment_weight_amt,
    a.facility_physician_num_attend,
    a.facility_physician_num_admit,
    a.facility_physician_num_pcp,
    a.payor_dw_id_ins1,
    a.iplan_id_ins1,
    a.ins_contract_id_ins1,
    a.major_payor_id_ins1,
    a.masterfacts_schema_id,
    a.ins_product_code_ins1,
    a.ins_product_name_ins1,
    a.sub_payor_group_code,
    a.payor_dw_id_ins2,
    a.iplan_id_ins2,
    a.payor_dw_id_ins3,
    a.iplan_id_ins3,
    a.calculated_los,
    a.total_account_balance_amt,
    a.total_billed_charges,
    a.total_anc_charges,
    a.total_rb_charges,
    a.total_payments,
    a.total_adjustment_amt,
    a.total_contract_allow_amt,
    a.total_write_off_bad_debt_amt,
    a.total_write_off_other_amt,
    a.total_write_off_other_adj_amt,
    a.total_uninsured_discount_amt,
    a.last_write_off_date,
    a.total_charity_write_off_amt,
    a.last_charity_write_off_date,
    a.total_patient_liability_amt,
    a.total_patient_payment_amt,
    a.last_patient_payment_date,
    a.exempt_indicator,
    a.casemix_exempt_indicator,
    a.expected_pmt,
    a.primary_icd_version_code,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.fact_patient AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
