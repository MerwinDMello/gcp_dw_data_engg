-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_appeal_var_adj.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_appeal_var_adj AS SELECT
    ref_cc_appeal_var_adj.company_code,
    ref_cc_appeal_var_adj.coid,
    ref_cc_appeal_var_adj.appeal_var_adj_code_id,
    ref_cc_appeal_var_adj.appeal_var_adj_code,
    ref_cc_appeal_var_adj.appeal_var_adj_code_desc,
    ref_cc_appeal_var_adj.appeal_var_adj_eff_end_date,
    ref_cc_appeal_var_adj.dw_last_update_date_time,
    ref_cc_appeal_var_adj.source_system_code
  FROM
    {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_appeal_var_adj
;
