-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sbs_otd_recovery_goals.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sbs_otd_recovery_goals AS SELECT
    CAST(bqutil.fn.cw_td_normalize_number(a.rptg_period) as INT64) AS date_sid,
    a.unit_num,
    a.coid,
    a.agency_name,
    substr(a.agency_type, 4, 1) AS otd_recovery_flag,
    substr(a.agency_type, 5, 1) AS same_store_flag,
    CAST(bqutil.fn.cw_td_normalize_number(substr(a.agency_type, 6, 2)) as INT64) AS financial_class,
    a.recovery_amt AS goal
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries AS a
  WHERE upper(a.agency_type) LIKE 'JH_%'
;
