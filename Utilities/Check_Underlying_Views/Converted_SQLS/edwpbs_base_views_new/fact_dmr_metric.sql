-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_dmr_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric AS SELECT
    fact_dmr_metric.service_type_name,
    fact_dmr_metric.fact_lvl_code,
    fact_dmr_metric.parent_code,
    fact_dmr_metric.child_code,
    fact_dmr_metric.rptg_date,
    fact_dmr_metric.dmr_day_month_ind,
    fact_dmr_metric.dmr_code,
    fact_dmr_metric.dmr_metric_code,
    fact_dmr_metric.dmr_patient_type_code,
    fact_dmr_metric.dmr_fin_class_code,
    fact_dmr_metric.sub_unit_num,
    fact_dmr_metric.dmr_metric_value,
    fact_dmr_metric.source_system_code,
    fact_dmr_metric.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_dmr_metric
;
