-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_month.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwfs_base_views.lu_month
;
