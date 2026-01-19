-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ar_charge_ps.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ar_charge_ps AS SELECT
    a.coid,
    a.company_code,
    a.sub_unit_num,
    a.charge_code,
    a.ar_dept_num,
    a.pe_date,
    a.ip_charge_rate_amt,
    a.op_charge_rate_amt,
    a.ip_cm_cy_unit_cnt,
    a.op_cm_cy_unit_cnt,
    a.ip_cm_cy_charge_amt,
    a.op_cm_cy_charge_amt,
    a.cm_cy_total_charge_amt,
    a.ip_ytd_unit_cnt,
    a.op_ytd_unit_cnt,
    a.ip_ytd_charge_amt,
    a.op_ytd_charge_amt,
    a.ytd_total_charge_amt,
    a.data_server_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_charge_ps AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
