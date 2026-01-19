-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_base_views/cc_facility_iplan_contract.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_base_views_dataset_name }}.cc_facility_iplan_contract
   OPTIONS(description='Data in this table will join Concuity Facility Iplan (Payer) information with Concuity Contract (Rate Schedule Profiles) information.')
  AS SELECT
      cc_facility_iplan_contract.company_code,
      cc_facility_iplan_contract.coid,
      cc_facility_iplan_contract.payor_dw_id,
      cc_facility_iplan_contract.cers_profile_id,
      cc_facility_iplan_contract.contract_effective_start_date,
      cc_facility_iplan_contract.patient_type_code,
      cc_facility_iplan_contract.contract_effective_end_date,
      cc_facility_iplan_contract.last_updated_date,
      cc_facility_iplan_contract.last_updated_by_uid,
      cc_facility_iplan_contract.dw_last_update_date_time,
      cc_facility_iplan_contract.source_system_code
    FROM
      {{ params.param_parallon_ra_core_dataset_name }}.cc_facility_iplan_contract
  ;
