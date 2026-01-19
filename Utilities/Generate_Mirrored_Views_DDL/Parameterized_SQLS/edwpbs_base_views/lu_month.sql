-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_month.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.lu_month AS SELECT
    lu_month.month_id,
    lu_month.month_id_desc_s,
    lu_month.month_id_desc_l,
    lu_month.month_num,
    lu_month.month_num_desc_s,
    lu_month.month_num_desc_l,
    lu_month.qtr_id AS quarter_id,
    lu_month.qtr_desc AS quarter_desc,
    lu_month.qtr_num AS quarter_num,
    lu_month.qtr_num_desc_s AS quarter_num_desc_s,
    lu_month.qtr_num_desc_l AS quarter_num_desc_l,
    lu_month.semi_annual_id,
    lu_month.semi_annual_desc,
    lu_month.year_id
  FROM
    {{ params.param_auth_base_views_dataset_name }}.lu_month
;
