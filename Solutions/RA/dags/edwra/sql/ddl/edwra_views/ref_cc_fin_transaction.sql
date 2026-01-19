-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_fin_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_fin_transaction
   OPTIONS(description='Reference Concuity Financial Transaction')
  AS SELECT
      a.company_code,
      a.coid,
      a.transaction_code,
      a.eff_begin_date,
      a.unit_num,
      a.transaction_type,
      a.transaction_desc,
      a.transaction_category_id,
      a.transaction_master_id,
      a.inactive_date,
      a.create_user_id,
      a.create_date_time,
      a.update_user_id,
      a.update_date_time,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_fin_transaction AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
