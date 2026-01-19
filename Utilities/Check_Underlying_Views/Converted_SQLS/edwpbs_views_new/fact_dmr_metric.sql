-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_dmr_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric AS SELECT
    fact_dmr_metric.service_type_name,
    fact_dmr_metric.fact_lvl_code,
    fact_dmr_metric.parent_code,
    fact_dmr_metric.child_code,
    fact_dmr_metric.rptg_date,
    fact_dmr_metric.dmr_day_month_ind,
    fact_dmr_metric.dmr_metric_code,
    fact_dmr_metric.dmr_code,
    fact_dmr_metric.dmr_patient_type_code,
    fact_dmr_metric.dmr_fin_class_code,
    fact_dmr_metric.sub_unit_num,
    fact_dmr_metric.dmr_metric_value,
    fact_dmr_metric.dw_last_update_date_time,
    fact_dmr_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric
  WHERE fact_dmr_metric.sub_unit_num = '00000'
UNION DISTINCT
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.child_code,
    a.rptg_date AS rptg_date,
    'DPMTH' AS dmr_day_month_ind,
    a.dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    a.sub_unit_num,
    b.dmr_metric_value,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND upper(a.child_code) = upper(b.child_code)
     AND b.rptg_date = date_add(a.rptg_date, interval -1 MONTH)
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND a.dmr_day_month_ind IN(
      'DCMTH', 'DCMTD'
    )
     AND a.dmr_metric_code = b.dmr_metric_code
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
     AND a.sub_unit_num = b.sub_unit_num
  WHERE a.sub_unit_num = '00000'
;
