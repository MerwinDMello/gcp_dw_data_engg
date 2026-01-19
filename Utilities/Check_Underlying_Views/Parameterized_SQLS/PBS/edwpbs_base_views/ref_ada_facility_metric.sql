-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ada_facility_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_ada_facility_metric
   OPTIONS(description='Reference Metrics table containing the percentages for Accrual calculations from ADA AR  Application for all Summary 7 facilities.')
  AS SELECT
      ref_ada_facility_metric.company_code,
      ref_ada_facility_metric.coid,
      ref_ada_facility_metric.eff_from_date,
      ref_ada_facility_metric.eff_to_date,
      ref_ada_facility_metric.unit_num,
      ROUND(ref_ada_facility_metric.ada_pct, 4, 'ROUND_HALF_EVEN') AS ada_pct,
      ROUND(ref_ada_facility_metric.secondary_pct, 4, 'ROUND_HALF_EVEN') AS secondary_pct,
      ROUND(ref_ada_facility_metric.charity_pct, 4, 'ROUND_HALF_EVEN') AS charity_pct,
      ROUND(ref_ada_facility_metric.uninsured_pct, 4, 'ROUND_HALF_EVEN') AS uninsured_pct,
      ROUND(ref_ada_facility_metric.spca_pct, 4, 'ROUND_HALF_EVEN') AS spca_pct,
      ref_ada_facility_metric.journal_entry_ind,
      ref_ada_facility_metric.source_system_code,
      ref_ada_facility_metric.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.ref_ada_facility_metric
  ;
