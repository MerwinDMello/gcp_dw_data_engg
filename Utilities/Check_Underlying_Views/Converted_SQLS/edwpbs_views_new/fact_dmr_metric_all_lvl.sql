-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_dmr_metric_all_lvl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS SELECT
    max(b.service_type_name) AS service_type_name,
    b.fact_lvl_code,
    b.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    a.dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    sum(a.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON a.parent_code = b.coid
  GROUP BY upper(b.service_type_name), 2, 3, 4, 5, 6, 7, 8, 9
;
