-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_remittance_additional_payee.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.junc_remittance_additional_payee
   OPTIONS(description='Crosswalk table for Payment Records and Additional Payee Details')
  AS SELECT
      a.payment_guid,
      a.additional_payee_line_num,
      a.remittance_additional_payee_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.junc_remittance_additional_payee AS a
  ;
