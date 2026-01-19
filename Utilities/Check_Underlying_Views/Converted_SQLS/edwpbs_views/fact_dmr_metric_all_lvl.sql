-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_dmr_metric_all_lvl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS SELECT
    max(b.service_type_name) AS service_type_name,
    b.fact_lvl_code,
    max(b.parent_code) AS parent_code,
    a.rptg_date,
    max(a.dmr_day_month_ind) AS dmr_day_month_ind,
    max(a.dmr_metric_code) AS dmr_metric_code,
    max(a.dmr_code) AS dmr_code,
    max(a.dmr_patient_type_code) AS dmr_patient_type_code,
    max(a.dmr_fin_class_code) AS dmr_fin_class_code,
    sum(a.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS b ON upper(a.parent_code) = upper(b.coid)
  GROUP BY upper(b.service_type_name), 2, upper(b.parent_code), 4, upper(a.dmr_day_month_ind), upper(a.dmr_metric_code), upper(a.dmr_code), upper(a.dmr_patient_type_code), upper(a.dmr_fin_class_code)
;
