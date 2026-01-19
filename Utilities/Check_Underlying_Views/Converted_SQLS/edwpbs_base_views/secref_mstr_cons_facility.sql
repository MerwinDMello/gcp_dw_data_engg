-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_mstr_cons_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_mstr_cons_facility AS SELECT
    secref_mstr_cons_facility.service_type_name,
    secref_mstr_cons_facility.fact_lvl_code,
    secref_mstr_cons_facility.parent_code,
    secref_mstr_cons_facility.user_id,
    secref_mstr_cons_facility.default_ind,
    secref_mstr_cons_facility.status_flag,
    secref_mstr_cons_facility.dw_last_update_date_time,
    secref_mstr_cons_facility.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_mstr_cons_facility
;
