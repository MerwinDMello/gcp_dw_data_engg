-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payor_employer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payor_employer
   OPTIONS(description='This table captures employer and employee relationship for the insurance reported.')
  AS SELECT
      a.company_code,
      a.coid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      a.employee_code,
      a.iplan_insurance_order_num,
      a.unit_num,
      a.employee_id,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.employee_work_area_code_num,
      a.employee_work_telephone,
      a.employment_status_code,
      a.employer_area_code_num,
      a.employer_name,
      a.employer_short_name,
      a.employer_street1_address,
      a.employer_street2_address,
      a.employer_city_name,
      a.employer_state_code,
      a.employer_zip_code,
      a.employer_telephone_num,
      a.length_of_employment_num,
      a.occupation_desc,
      a.pa_last_update_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
