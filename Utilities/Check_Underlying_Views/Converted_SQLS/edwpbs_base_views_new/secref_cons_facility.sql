-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_cons_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_cons_facility AS SELECT
    secref_cons_facility.company_code,
    secref_cons_facility.user_id,
    secref_cons_facility.facility_number
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_cons_facility
;
