-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_dmr_code.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.lu_dmr_code AS SELECT
    lu_dmr_code.dmr_code,
    lu_dmr_code.dmr_desc,
    lu_dmr_code.dmr_type_code,
    lu_dmr_code.dw_last_update_date_time,
    lu_dmr_code.source_system_code
  FROM
    {{ params.param_pbs_core_dataset_name }}.lu_dmr_code
;
