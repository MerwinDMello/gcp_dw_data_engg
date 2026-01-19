-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/registration_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.registration_pf AS SELECT
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.pat_acct_num,
    a.site_id_code,
    a.site_id_override_code,
    a.registrar_user_id,
    a.pa_facility_unit_id,
    a.facility_sub_unit_id,
    a.registration_priority_code,
    a.vip_ind,
    a.confidentiality_ind,
    a.last_hosp_facility_name,
    a.last_hosp_begin_date,
    a.last_hosp_end_date,
    a.registration_comment_text,
    a.medical_record_num,
    a.registration_reason_text,
    a.religious_affiliation_text,
    a.clergy_visitation_ind,
    a.total_account_balance_amt,
    a.grouper_name,
    a.pa_restore_date,
    a.clear_contract_ind,
    a.special_bill_ind,
    a.insurance_maint_date,
    a.init_bad_debt_prelist_date,
    a.bad_debt_reason_code,
    a.apc_from_date,
    a.auto_discharge_ind,
    a.statement_suppress_ind,
    a.primary_icd_version_code,
    a.solis_dept_flag,
    a.soli_service_flag,
    a.patient_type_claim_diff_flag,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.registration_pf AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
