-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/ref_cc_ce_service.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_ce_service
   OPTIONS(description='Reference table to services performed by the Concuity calculation engine.')
  AS SELECT
      ref_cc_ce_service.company_code,
      ref_cc_ce_service.coid,
      ref_cc_ce_service.ce_service_id,
      ref_cc_ce_service.ce_service_name,
      ref_cc_ce_service.doc_service_ind,
      ref_cc_ce_service.pass_through_ind,
      ref_cc_ce_service.pass_through_active_ind,
      ref_cc_ce_service.ce_service_create_date_time,
      ref_cc_ce_service.ce_service_update_date_time,
      ref_cc_ce_service.source_system_code,
      ref_cc_ce_service.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_ce_service
  ;
