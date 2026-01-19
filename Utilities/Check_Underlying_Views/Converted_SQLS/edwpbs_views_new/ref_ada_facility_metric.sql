-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_ada_facility_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_ada_facility_metric AS SELECT
    a.company_code,
    a.coid,
    a.eff_from_date,
    a.eff_to_date,
    a.unit_num,
    a.ada_pct,
    a.secondary_pct,
    a.charity_pct,
    a.uninsured_pct,
    a.spca_pct,
    a.journal_entry_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ada_facility_metric AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
