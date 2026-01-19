-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_corrected_priority_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.ref_remittance_corrected_priority_payor
   OPTIONS(description='Reference table to maintain th eCorrected Priority Payor details of the claims sent.')
  AS SELECT
      a.corrected_priority_payor_sid,
      a.corrected_priority_payor_qualifier_code,
      a.corrected_priority_payor_id,
      a.corrected_priority_payor_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.ref_remittance_corrected_priority_payor AS a
  ;
