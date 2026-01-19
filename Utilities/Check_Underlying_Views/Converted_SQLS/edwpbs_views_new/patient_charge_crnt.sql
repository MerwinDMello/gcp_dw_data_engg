-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_charge_crnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_charge_crnt AS SELECT
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.charge_code,
    a.service_date,
    a.charge_posted_date_time,
    a.origin_id,
    a.room_bed_num,
    a.revenue_code,
    a.credit_ind,
    a.pat_acct_num,
    a.gl_dept_num,
    a.gl_sub_account_num,
    a.mnemonic_code,
    a.charge_amt,
    a.charge_desc,
    a.manual_change_ind,
    a.factor_qty,
    a.professional_component_ind,
    a.batch_day_num,
    a.batch_month_num,
    a.patient_charge_type_code,
    a.facility_physician_intr_num,
    a.facility_physician_ord_num,
    a.charge_posted_date,
    a.charge_posted_date_time_ts,
    a.pe_date,
    a.dept_num,
    a.non_covered_charge_flag,
    a.fee_schedule_code,
    a.place_of_service_code,
    a.non_bill_reason_code,
    a.non_bill_gl_acct_num,
    a.non_bill_manual_update_ind,
    a.bill_through_date,
    a.partb_charge_ind_ins1,
    a.partb_charge_ind_ins2,
    a.partb_charge_ind_ins3,
    a.original_service_date,
    a.distributed_system_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_charge_mv_pf AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
