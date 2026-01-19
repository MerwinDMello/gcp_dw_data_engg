-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payor_employer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payor_employer AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.iplan_id,
    a.employee_code,
    a.iplan_insurance_order_num,
    a.unit_num,
    a.employee_id,
    a.pat_acct_num,
    a.payor_dw_id,
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
