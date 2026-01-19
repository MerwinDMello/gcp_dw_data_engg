-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_facility_iplan_contract.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_facility_iplan_contract
   OPTIONS(description='Data in this table will join Concuity Facility Iplan (Payer) information with Concuity Contract (Rate Schedule Profiles) information.')
  AS SELECT
      a.company_code,
      a.coid,
      a.payor_dw_id,
      a.cers_profile_id,
      a.contract_effective_start_date,
      a.patient_type_code,
      a.contract_effective_end_date,
      a.last_updated_date,
      a.last_updated_by_uid,
      a.dw_last_update_date_time,
      a.source_system_code
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_facility_iplan_contract AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
