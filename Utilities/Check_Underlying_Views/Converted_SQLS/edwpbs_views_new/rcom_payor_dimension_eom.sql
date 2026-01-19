-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/rcom_payor_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.rcom_payor_dimension_eom AS SELECT
    a.payor_dw_id,
    a.pe_date,
    a.parent_payor_name,
    a.sub_payor_name,
    a.payor_short_name,
    a.product_name,
    a.contract_type_code,
    a.payor_id,
    a.eff_from_date,
    a.eff_to_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom AS a
;
