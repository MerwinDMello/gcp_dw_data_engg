-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_employer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.payor_employer
   OPTIONS(description='This table captures employer and employee relationship for the insurance reported.')
  AS SELECT
      payor_employer.company_code,
      payor_employer.coid,
      ROUND(payor_employer.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payor_employer.iplan_id,
      payor_employer.employee_code,
      payor_employer.iplan_insurance_order_num,
      payor_employer.unit_num,
      payor_employer.employee_id,
      ROUND(payor_employer.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(payor_employer.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      payor_employer.employee_work_area_code_num,
      payor_employer.employee_work_telephone,
      payor_employer.employment_status_code,
      payor_employer.employer_area_code_num,
      payor_employer.employer_name,
      payor_employer.employer_short_name,
      payor_employer.employer_street1_address,
      payor_employer.employer_street2_address,
      payor_employer.employer_city_name,
      payor_employer.employer_state_code,
      payor_employer.employer_zip_code,
      payor_employer.employer_telephone_num,
      payor_employer.length_of_employment_num,
      payor_employer.occupation_desc,
      payor_employer.pa_last_update_date,
      payor_employer.source_system_code,
      payor_employer.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.payor_employer
  ;
