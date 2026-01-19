-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_armap_facility_goal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_armap_facility_goal AS SELECT
    lu_armap_facility_goal.year_id,
    lu_armap_facility_goal.coid,
    lu_armap_facility_goal.unit_num,
    ROUND(lu_armap_facility_goal.upfront_collection_pcnt, 5, 'ROUND_HALF_EVEN') AS upfront_collection_pcnt,
    lu_armap_facility_goal.dw_last_update_date_time,
    lu_armap_facility_goal.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_facility_goal
;
