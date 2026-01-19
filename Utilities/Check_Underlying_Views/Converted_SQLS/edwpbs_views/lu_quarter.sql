-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_quarter.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_quarter AS SELECT
    lu_quarter.qtr_id AS quarter_id,
    lu_quarter.qtr_desc AS quarter_desc,
    lu_quarter.qtr_num AS quarter_num,
    lu_quarter.qtr_num_desc_s AS quarter_num_desc_s,
    lu_quarter.qtr_num_desc_l AS quarter_num_desc_l,
    lu_quarter.semi_annual_id,
    lu_quarter.semi_annual_desc,
    lu_quarter.year_id
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.lu_quarter
;
