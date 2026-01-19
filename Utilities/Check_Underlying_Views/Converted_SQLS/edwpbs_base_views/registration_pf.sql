-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/registration_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.registration_pf AS SELECT
    registration.coid,
    registration.company_code,
    ROUND(registration.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(registration.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    registration.site_id_code,
    registration.site_id_override_code,
    ROUND(registration.registrar_user_id, 0, 'ROUND_HALF_EVEN') AS registrar_user_id,
    registration.pa_facility_unit_id,
    registration.facility_sub_unit_id,
    registration.registration_priority_code,
    registration.vip_ind,
    registration.confidentiality_ind,
    registration.last_hosp_facility_name,
    registration.last_hosp_begin_date,
    registration.last_hosp_end_date,
    registration.registration_comment_text,
    registration.medical_record_num,
    registration.registration_reason_text,
    registration.religious_affiliation_text,
    registration.clergy_visitation_ind,
    ROUND(registration.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    registration.grouper_name,
    registration.pa_restore_date,
    registration.clear_contract_ind,
    registration.special_bill_ind,
    registration.insurance_maint_date,
    registration.init_bad_debt_prelist_date,
    registration.bad_debt_reason_code,
    registration.apc_from_date,
    registration.auto_discharge_ind,
    registration.statement_suppress_ind,
    registration.primary_icd_version_code,
    registration.solis_dept_flag,
    registration.soli_service_flag,
    registration.patient_type_claim_diff_flag,
    registration.source_system_code,
    registration.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.registration
;
