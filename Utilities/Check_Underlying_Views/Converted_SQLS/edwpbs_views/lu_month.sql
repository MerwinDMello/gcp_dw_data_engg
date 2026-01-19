-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_month.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_month AS SELECT
    lu_month.month_id,
    lu_month.month_id_desc_s,
    lu_month.month_id_desc_l,
    lu_month.month_num,
    lu_month.month_num_desc_s,
    lu_month.month_num_desc_l,
    lu_month.quarter_id,
    lu_month.quarter_desc,
    lu_month.quarter_num,
    lu_month.quarter_num_desc_s,
    lu_month.quarter_num_desc_l,
    lu_month.semi_annual_id,
    lu_month.semi_annual_desc,
    lu_month.year_id
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month
;
