-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/ref_cc_org_structure.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.ref_cc_org_structure
   OPTIONS(description='Reference to all HCA and NON-HCA facilites to get company code, coid, other data elements in regards to reporting.')
  AS SELECT
      a.company_code,
      a.coid,
      a.unit_num,
      a.schema_id,
      a.org_id,
      a.customer_id,
      a.active_ind,
      a.schema_name,
      a.customer_name,
      a.ssc_name,
      a.facility_name,
      a.org_status,
      a.create_date_time,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
