-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_agency_placement_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.collection_agency_placement_detail
   OPTIONS(description='This table holds the agency placement details of all collections from Artiva  for any accounts')
  AS SELECT
      ROUND(a.placement_sid, 0, 'ROUND_HALF_EVEN') AS placement_sid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.company_code,
      a.coid,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.unit_num,
      a.vendor_id,
      a.artiva_instance_code,
      a.placement_date,
      a.placement_time,
      a.recall_reason_code,
      a.recall_date,
      a.recall_time,
      ROUND(a.placement_amt, 3, 'ROUND_HALF_EVEN') AS placement_amt,
      ROUND(a.recall_amt, 3, 'ROUND_HALF_EVEN') AS recall_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.collection_agency_placement_detail AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
