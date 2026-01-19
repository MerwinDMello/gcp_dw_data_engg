-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_fin_transaction.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_fin_transaction
   OPTIONS(description='Reference Concuity Financial Transaction')
  AS SELECT
      ref_cc_fin_transaction.company_code,
      ref_cc_fin_transaction.coid,
      ref_cc_fin_transaction.transaction_code,
      ref_cc_fin_transaction.eff_begin_date,
      ref_cc_fin_transaction.unit_num,
      ref_cc_fin_transaction.transaction_type,
      ref_cc_fin_transaction.transaction_desc,
      ref_cc_fin_transaction.transaction_category_id,
      ref_cc_fin_transaction.transaction_master_id,
      ref_cc_fin_transaction.inactive_date,
      ref_cc_fin_transaction.create_user_id,
      ref_cc_fin_transaction.create_date_time,
      ref_cc_fin_transaction.update_user_id,
      ref_cc_fin_transaction.update_date_time,
      ref_cc_fin_transaction.dw_last_update_date_time,
      ref_cc_fin_transaction.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_fin_transaction
  ;
