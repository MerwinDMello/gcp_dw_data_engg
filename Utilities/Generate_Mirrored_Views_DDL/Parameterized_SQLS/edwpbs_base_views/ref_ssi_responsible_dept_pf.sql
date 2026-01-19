-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ssi_responsible_dept_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.ref_ssi_responsible_dept_pf AS SELECT
    ref_ssi_responsible_dept.ssi_queue_dept_id,
    ref_ssi_responsible_dept.ssi_queue_dept_desc,
    ref_ssi_responsible_dept.source_system_code
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_ssi_responsible_dept
;
