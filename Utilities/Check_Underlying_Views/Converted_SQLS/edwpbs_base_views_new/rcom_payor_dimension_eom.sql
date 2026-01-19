-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rcom_payor_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom AS SELECT
    rcom_payor_dimension_eom.payor_dw_id,
    rcom_payor_dimension_eom.pe_date,
    rcom_payor_dimension_eom.parent_payor_name,
    rcom_payor_dimension_eom.sub_payor_name,
    rcom_payor_dimension_eom.payor_short_name,
    rcom_payor_dimension_eom.product_name,
    rcom_payor_dimension_eom.contract_type_code,
    rcom_payor_dimension_eom.payor_id,
    rcom_payor_dimension_eom.eff_from_date,
    rcom_payor_dimension_eom.eff_to_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.rcom_payor_dimension_eom
;
