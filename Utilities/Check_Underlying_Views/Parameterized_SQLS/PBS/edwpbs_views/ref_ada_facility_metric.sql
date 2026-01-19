-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_ada_facility_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_ada_facility_metric
   OPTIONS(description='Reference Metrics table containing the percentages for Accrual calculations from ADA AR  Application for all Summary 7 facilities.')
  AS SELECT
      a.company_code,
      a.coid,
      a.eff_from_date,
      a.eff_to_date,
      a.unit_num,
      ROUND(a.ada_pct, 4, 'ROUND_HALF_EVEN') AS ada_pct,
      ROUND(a.secondary_pct, 4, 'ROUND_HALF_EVEN') AS secondary_pct,
      ROUND(a.charity_pct, 4, 'ROUND_HALF_EVEN') AS charity_pct,
      ROUND(a.uninsured_pct, 4, 'ROUND_HALF_EVEN') AS uninsured_pct,
      ROUND(a.spca_pct, 4, 'ROUND_HALF_EVEN') AS spca_pct,
      a.journal_entry_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_ada_facility_metric AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
