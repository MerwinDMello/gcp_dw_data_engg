-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/rcom_payor_dimension_eom.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.rcom_payor_dimension_eom
   OPTIONS(description='Payor Dimension Table End of Month Snapshot having Parent Payor mappings')
  AS SELECT
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
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
      {{ params.param_pbs_base_views_dataset_name }}.rcom_payor_dimension_eom AS a
  ;
