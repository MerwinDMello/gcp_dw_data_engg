-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_dcrp_unit_cost_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.eis_dcrp_unit_cost_year_dim AS SELECT
    a.coid,
    a.company_code,
    a.cost_year_beg_date,
    a.source_sid,
    a.unit_num_sid,
    a.year_created_sid,
    a.cost_year_end_date
  FROM
    {{ params.param_pbs_base_views_dataset_name }}.eis_dcrp_unit_cost_year_dim AS a
    CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b
  WHERE upper(b.co_id) = upper(a.coid)
   AND upper(b.company_code) = upper(a.company_code)
   AND b.user_id = session_user()
;
