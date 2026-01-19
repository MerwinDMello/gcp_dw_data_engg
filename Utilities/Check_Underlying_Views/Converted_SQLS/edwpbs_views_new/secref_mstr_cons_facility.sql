-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/secref_mstr_cons_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.secref_mstr_cons_facility AS SELECT
    secref_mstr_cons_facility.service_type_name,
    secref_mstr_cons_facility.fact_lvl_code,
    secref_mstr_cons_facility.parent_code,
    secref_mstr_cons_facility.user_id,
    secref_mstr_cons_facility.default_ind,
    secref_mstr_cons_facility.dw_last_update_date_time,
    secref_mstr_cons_facility.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_mstr_cons_facility
  WHERE secref_mstr_cons_facility.status_flag = 'Y'
;
