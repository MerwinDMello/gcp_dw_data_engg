-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ada_facility_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ada_facility_metric AS SELECT
    ref_ada_facility_metric.company_code,
    ref_ada_facility_metric.coid,
    ref_ada_facility_metric.eff_from_date,
    ref_ada_facility_metric.eff_to_date,
    ref_ada_facility_metric.unit_num,
    ref_ada_facility_metric.ada_pct,
    ref_ada_facility_metric.secondary_pct,
    ref_ada_facility_metric.charity_pct,
    ref_ada_facility_metric.uninsured_pct,
    ref_ada_facility_metric.spca_pct,
    ref_ada_facility_metric.journal_entry_ind,
    ref_ada_facility_metric.source_system_code,
    ref_ada_facility_metric.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_ada_facility_metric
;
