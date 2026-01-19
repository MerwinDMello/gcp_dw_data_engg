-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/registration_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.registration_pf AS SELECT
    registration.coid,
    registration.company_code,
    registration.patient_dw_id,
    registration.pat_acct_num,
    registration.site_id_code,
    registration.site_id_override_code,
    registration.registrar_user_id,
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
    registration.total_account_balance_amt,
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
