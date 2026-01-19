-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_charge_ps.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_charge_ps AS SELECT
    a.coid,
    a.company_code,
    a.sub_unit_num,
    a.charge_code,
    a.ar_dept_num,
    a.pe_date,
    ROUND(a.ip_charge_rate_amt, 4, 'ROUND_HALF_EVEN') AS ip_charge_rate_amt,
    ROUND(a.op_charge_rate_amt, 4, 'ROUND_HALF_EVEN') AS op_charge_rate_amt,
    a.ip_cm_cy_unit_cnt,
    a.op_cm_cy_unit_cnt,
    ROUND(a.ip_cm_cy_charge_amt, 4, 'ROUND_HALF_EVEN') AS ip_cm_cy_charge_amt,
    ROUND(a.op_cm_cy_charge_amt, 4, 'ROUND_HALF_EVEN') AS op_cm_cy_charge_amt,
    ROUND(a.cm_cy_total_charge_amt, 4, 'ROUND_HALF_EVEN') AS cm_cy_total_charge_amt,
    a.ip_ytd_unit_cnt,
    a.op_ytd_unit_cnt,
    ROUND(a.ip_ytd_charge_amt, 4, 'ROUND_HALF_EVEN') AS ip_ytd_charge_amt,
    ROUND(a.op_ytd_charge_amt, 4, 'ROUND_HALF_EVEN') AS op_ytd_charge_amt,
    ROUND(a.ytd_total_charge_amt, 4, 'ROUND_HALF_EVEN') AS ytd_total_charge_amt,
    a.data_server_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.ar_charge AS a
;
