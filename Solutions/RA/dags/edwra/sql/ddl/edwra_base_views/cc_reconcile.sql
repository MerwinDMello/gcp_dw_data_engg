-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_reconcile.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_reconcile AS SELECT
    cc_reconcile.schema_id,
    cc_reconcile.cc_table_name,
    cc_reconcile.coid,
    cc_reconcile.cc_reconcile_id,
    cc_reconcile.created_date_time,
    cc_reconcile.cc_reconcile_desc,
    cc_reconcile.expected_row_cnt,
    cc_reconcile.actual_row_cnt,
    cc_reconcile.updated_date_time
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.cc_reconcile
;
