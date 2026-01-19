-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_armap_facility_goal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_armap_facility_goal AS SELECT
    lu_armap_facility_goal.year_id,
    lu_armap_facility_goal.coid,
    lu_armap_facility_goal.unit_num,
    lu_armap_facility_goal.upfront_collection_pcnt,
    lu_armap_facility_goal.dw_last_update_date_time,
    lu_armap_facility_goal.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_facility_goal
;
