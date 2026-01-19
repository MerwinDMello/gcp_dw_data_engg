-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payor_employer.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payor_employer AS SELECT
    payor_employer.company_code,
    payor_employer.coid,
    payor_employer.patient_dw_id,
    payor_employer.iplan_id,
    payor_employer.employee_code,
    payor_employer.iplan_insurance_order_num,
    payor_employer.unit_num,
    payor_employer.employee_id,
    payor_employer.pat_acct_num,
    payor_employer.payor_dw_id,
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
    `hca-hin-dev-cur-parallon`.edwpbs.payor_employer
;
