-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_hsc_delinquency_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_hsc_delinquency_rate AS SELECT
    a.company_code,
    a.coid,
    a.month_id,
    a.patient_type_code_pos1,
    a.unit_num,
    a.charts_not_revw_thty_day_cnt,
    a.total_discharge_cnt,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_hsc_delinquency_rate AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
