-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_procedure_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_procedure_pf AS SELECT
    a.coid,
    a.company_code,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    a.procedure_code,
    a.procedure_type_code,
    a.service_date,
    ROUND(a.procedure_seq_num, 0, 'ROUND_HALF_EVEN') AS procedure_seq_num,
    ROUND(a.pa_procedure_seq_num, 0, 'ROUND_HALF_EVEN') AS pa_procedure_seq_num,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.charge_code,
    a.revenue_code,
    ROUND(a.charge_posted_date_time, 0, 'ROUND_HALF_EVEN') AS charge_posted_date_time,
    a.procedure_service_time,
    a.eff_to_date,
    a.procedure_rank_num,
    a.origin_id,
    a.facility_physician_intr_num,
    a.facility_physician_ord_num,
    a.facility_physician_surg_num,
    a.hcpcs_procedure_modifier_code1,
    a.hcpcs_procedure_modifier_code2,
    a.hcpcs_procedure_modifier_code3,
    a.hcpcs_procedure_modifier_code4,
    a.hosp_acquired_cond_code,
    a.composite_adjustment_flag,
    a.eff_from_date,
    a.procedure_mapped_code,
    a.dss_op_cpt_hier,
    a.aps_op_cpt_reimb_hier_id,
    a.non_covered_billed_flag,
    a.procedure_affected_apr_drg_code,
    a.procedure_affected_drg_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.patient_procedure AS a
;
