-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_org_structure.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure
   OPTIONS(description='Reference to all HCA and NON-HCA facilites to get company code, coid, other data elements in regards to reporting.')
  AS SELECT
      ref_cc_org_structure.company_code,
      ref_cc_org_structure.coid,
      ref_cc_org_structure.unit_num,
      ref_cc_org_structure.schema_id,
      ref_cc_org_structure.org_id,
      ref_cc_org_structure.customer_id,
      ref_cc_org_structure.active_ind,
      ref_cc_org_structure.schema_name,
      ref_cc_org_structure.customer_name,
      ref_cc_org_structure.ssc_name,
      ref_cc_org_structure.facility_name,
      ref_cc_org_structure.org_status,
      ref_cc_org_structure.create_date_time,
      ref_cc_org_structure.source_system_code,
      ref_cc_org_structure.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure
  ;
