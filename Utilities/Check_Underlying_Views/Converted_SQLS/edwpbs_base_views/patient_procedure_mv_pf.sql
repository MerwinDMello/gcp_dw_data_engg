-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_procedure_mv_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_procedure_mv_pf AS SELECT
    patient_procedure_mv.coid,
    patient_procedure_mv.company_code,
    ROUND(patient_procedure_mv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    patient_procedure_mv.procedure_code,
    patient_procedure_mv.procedure_type_code,
    patient_procedure_mv.service_date,
    ROUND(patient_procedure_mv.procedure_seq_num, 0, 'ROUND_HALF_EVEN') AS procedure_seq_num,
    ROUND(patient_procedure_mv.pa_procedure_seq_num, 0, 'ROUND_HALF_EVEN') AS pa_procedure_seq_num,
    ROUND(patient_procedure_mv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    patient_procedure_mv.charge_code,
    patient_procedure_mv.revenue_code,
    ROUND(patient_procedure_mv.charge_posted_date_time, 0, 'ROUND_HALF_EVEN') AS charge_posted_date_time,
    patient_procedure_mv.procedure_service_time,
    patient_procedure_mv.eff_to_date,
    patient_procedure_mv.procedure_rank_num,
    patient_procedure_mv.origin_id,
    patient_procedure_mv.facility_physician_intr_num,
    patient_procedure_mv.facility_physician_ord_num,
    patient_procedure_mv.facility_physician_surg_num,
    patient_procedure_mv.hcpcs_procedure_modifier_code1,
    patient_procedure_mv.hcpcs_procedure_modifier_code2,
    patient_procedure_mv.hcpcs_procedure_modifier_code3,
    patient_procedure_mv.hcpcs_procedure_modifier_code4,
    patient_procedure_mv.hosp_acquired_cond_code,
    patient_procedure_mv.composite_adjustment_flag,
    patient_procedure_mv.eff_from_date,
    patient_procedure_mv.procedure_mapped_code,
    patient_procedure_mv.dss_op_cpt_hier,
    patient_procedure_mv.aps_op_cpt_reimb_hier_id,
    patient_procedure_mv.non_covered_billed_flag,
    patient_procedure_mv.procedure_affected_apr_drg_code,
    patient_procedure_mv.procedure_affected_drg_code,
    patient_procedure_mv.source_system_code,
    patient_procedure_mv.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_procedure_mv
;
