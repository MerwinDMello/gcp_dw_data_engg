-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_dmr_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_dmr_metric
   OPTIONS(description='This table measures many Daily Management report metrics used in Revenue Cycle Project.')
  AS SELECT
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
      ROUND(fact_dmr_metric.dmr_metric_value, 3, 'ROUND_HALF_EVEN') AS dmr_metric_value,
      fact_dmr_metric.source_system_code,
      fact_dmr_metric.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_dmr_metric
  ;
