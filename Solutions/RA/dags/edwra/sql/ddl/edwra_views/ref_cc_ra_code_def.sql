-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_ra_code_def.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_ra_code_def
   OPTIONS(description='Remittance Code Definition|Remittance Advice group codes, reason codes, status codes, and remark codes.|835 Related')
  AS SELECT
      a.company_code,
      a.coid,
      a.ra_code_def_id,
      a.ra_code_type,
      a.ra_code,
      a.ra_short_desc,
      a.ra_desc,
      a.create_date_time,
      a.update_date_time,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_ra_code_def AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
