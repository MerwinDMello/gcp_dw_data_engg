-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/rcom_payor_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom
   OPTIONS(description='Payor Dimension Table End of Month Snapshot having Parent Payor mappings')
  AS SELECT
      ROUND(rcom_payor_dimension_eom.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
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
