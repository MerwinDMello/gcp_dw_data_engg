-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sbs_otd_recovery_goals.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sbs_otd_recovery_goals AS SELECT
    CASE
       a.rptg_period
      WHEN '' THEN 0
      ELSE CAST(a.rptg_period as INT64)
    END AS date_sid,
    a.unit_num,
    a.coid,
    a.agency_name,
    substr(a.agency_type, 4, 1) AS otd_recovery_flag,
    substr(a.agency_type, 5, 1) AS same_store_flag,
    CASE
       substr(a.agency_type, 6, 2)
      WHEN '' THEN 0
      ELSE CAST(substr(a.agency_type, 6, 2) as INT64)
    END AS financial_class,
    ROUND(a.recovery_amt, 3, 'ROUND_HALF_EVEN') AS goal
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries AS a
  WHERE upper(a.agency_type) LIKE 'JH_%'
;
