-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_procedure_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_procedure_pf AS SELECT
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.procedure_code,
    a.procedure_type_code,
    a.service_date,
    a.procedure_seq_num,
    a.pa_procedure_seq_num,
    a.pat_acct_num,
    a.charge_code,
    a.revenue_code,
    a.charge_posted_date_time,
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
