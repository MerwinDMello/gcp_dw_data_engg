-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
