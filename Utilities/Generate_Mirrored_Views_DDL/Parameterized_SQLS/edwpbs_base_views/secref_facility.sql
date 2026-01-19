-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS {{ params.param_pbs_base_views_dataset_name }}.secref_facility AS SELECT
    secref_facility.company_code,
    secref_facility.user_id,
    secref_facility.co_id
  FROM
    {{ params.param_pbs_core_dataset_name }}.secref_facility
;
