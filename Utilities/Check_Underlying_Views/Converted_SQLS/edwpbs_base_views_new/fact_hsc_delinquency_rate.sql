-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_hsc_delinquency_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_hsc_delinquency_rate AS SELECT
    fact_hsc_delinquency_rate.company_code,
    fact_hsc_delinquency_rate.coid,
    fact_hsc_delinquency_rate.month_id,
    fact_hsc_delinquency_rate.patient_type_code_pos1,
    fact_hsc_delinquency_rate.unit_num,
    fact_hsc_delinquency_rate.charts_not_revw_thty_day_cnt,
    fact_hsc_delinquency_rate.total_discharge_cnt,
    fact_hsc_delinquency_rate.source_system_code,
    fact_hsc_delinquency_rate.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_hsc_delinquency_rate
;
