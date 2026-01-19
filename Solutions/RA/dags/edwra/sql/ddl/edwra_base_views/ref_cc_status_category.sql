-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_status_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_status_category AS SELECT
    ref_cc_status_category.company_code,
    ref_cc_status_category.coid,
    ref_cc_status_category.status_category_id,
    ref_cc_status_category.status_category_name,
    ref_cc_status_category.dw_last_update_date_time,
    ref_cc_status_category.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_status_category
;
