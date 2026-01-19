-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.collection_user
   OPTIONS(description='Standard Artiva Collections User file information')
  AS SELECT
      a.user_id,
      a.artiva_instance_code,
      a.user_dept_num,
      a.user_full_name,
      a.global_vendor_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.collection_user AS a
  ;
