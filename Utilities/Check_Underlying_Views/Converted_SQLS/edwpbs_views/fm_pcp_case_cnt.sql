-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fm_pcp_case_cnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fm_pcp_case_cnt AS SELECT
    max(fd.customer_code) AS customer_code,
    max(fd.ssc_code) AS ssc_code,
    max(CASE
      WHEN upper(pa.unit_num) = '08158'
       AND upper(pa.sub_unit_num) = '00005' THEN '08165'
      WHEN upper(pa.unit_num) = '00031'
       AND upper(pa.sub_unit_num) = '00002' THEN '00584'
      ELSE pa.unit_num
    END) AS unit_num,
    max(pa.admission_patient_type_code) AS admission_patient_type_code,
    max(pa.patient_type_code_pos1) AS patient_type_code_pos1,
    max(pt.admission_prev_pat_type_code) AS admission_prev_pat_type_code,
    php.facility_physician_num AS pcp_num,
    max(php.physician_name) AS pcp_name,
    phr.facility_physician_num AS referring_dr_num,
    max(phr.physician_name) AS referring_dr_name,
    max(CASE
      WHEN upper(substr(php.physician_name, 1, 4)) = 'NO P' THEN 'NO PCP'
      WHEN upper(substr(php.physician_name, 1, 12)) = 'PHYSICIAN NO' THEN 'NO PCP'
      WHEN upper(substr(php.physician_name, 1, 8)) = 'SELF REF' THEN 'NO PCP'
      WHEN upper(substr(php.physician_name, 1, 13)) = 'REFERRED SELF' THEN 'NO PCP'
      WHEN upper(substr(php.physician_name, 1, 8)) = 'DOES NOT' THEN 'DNK'
      WHEN upper(substr(php.physician_name, 1, 9)) = 'DOES_NOT' THEN 'DNK'
      WHEN upper(substr(php.physician_name, 1, 13)) = 'PROVIDER UNKN' THEN 'DNK'
      WHEN upper(substr(php.physician_name, 1, 6)) = 'URGENT' THEN '.URGENT'
      WHEN php.facility_physician_num = 0 THEN 'BLANK'
      WHEN php.facility_physician_num = 9999 THEN 'IDENTIFIED'
      WHEN php.facility_physician_num <> 9999 THEN 'IDENTIFIED'
      ELSE 'BLANK'
    END) AS pcp,
    max(CASE
      WHEN upper(substr(phr.physician_name, 1, 4)) = 'NO P' THEN 'DNK'
      WHEN upper(substr(phr.physician_name, 1, 8)) = 'SELF REF' THEN 'SELF'
      WHEN upper(substr(phr.physician_name, 1, 13)) = 'REFERRED SELF' THEN 'SELF'
      WHEN upper(substr(phr.physician_name, 1, 8)) = 'DOES NOT' THEN 'DNK'
      WHEN upper(substr(phr.physician_name, 1, 13)) = 'PROVIDER UNKN' THEN 'DNK'
      WHEN upper(substr(phr.physician_name, 1, 12)) = 'PHYSICIAN NO' THEN 'DNK'
      WHEN upper(substr(phr.physician_name, 1, 6)) = 'URGENT' THEN '.URGENT'
      WHEN phr.facility_physician_num = 0 THEN 'BLANK'
      WHEN phr.facility_physician_num = 9999 THEN 'IDENTIFIED'
      WHEN phr.facility_physician_num <> 9999 THEN 'IDENTIFIED'
      ELSE 'BLANK'
    END) AS ref,
    max(CASE
      WHEN upper(substr(pt.admission_prev_pat_type_code, 1, 1)) = 'E'
       AND upper(pa.patient_type_code_pos1) = 'I' THEN 'ED ADMITS'
      WHEN upper(pa.patient_type_code_pos1) = 'I' THEN 'NON-ED ADMITS'
      WHEN upper(pa.patient_type_code_pos1) = 'E' THEN 'ED TREAT & RELEASE'
      WHEN upper(pa.patient_type_code_pos1) = 'O' THEN 'OP TREAT & RELEASE'
      WHEN upper(pa.patient_type_code_pos1) = 'S' THEN 'SDC TREAT & RELEASE'
      ELSE CAST(NULL as STRING)
    END) AS type_of_admission,
    pa.admission_date,
    max(pa.coid) AS coid,
    ROUND(pa.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    count(pa.pat_acct_num) AS number_of_cases
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient AS pa
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_organization AS fd ON upper(fd.unit_num) = upper(CASE
      WHEN upper(pa.unit_num) = '08158'
       AND upper(pa.sub_unit_num) = '00005' THEN '08165'
      WHEN upper(pa.unit_num) = '00031'
       AND upper(pa.sub_unit_num) = '00002' THEN '00584'
      ELSE pa.unit_num
    END)
     AND upper(fd.customer_code) IN(
      'ARH', 'HCA'
    )
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_patient_type_pf AS pt ON pt.patient_dw_id = pa.patient_dw_id
     AND pt.eff_to_date = DATE '9999-12-31'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf AS php ON php.patient_dw_id = pa.patient_dw_id
     AND upper(php.role_type_code) = 'PCP'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf AS phr ON phr.patient_dw_id = pa.patient_dw_id
     AND upper(phr.role_type_code) = 'REF'
  WHERE pa.admission_date BETWEEN date_add(date_trunc(current_date('US/Central'), MONTH), interval -1 MONTH) AND date_sub(date_add(date_trunc(current_date('US/Central'), MONTH), interval 0 MONTH), interval 1 DAY)
   AND upper(pt.admission_patient_type_code) NOT IN(
    'OP', 'EP', 'IP', 'IB'
  )
   AND upper(pa.patient_type_code_pos1) IN(
    'I', 'E', 'O', 'S'
  )
  GROUP BY upper(fd.customer_code), upper(fd.ssc_code), upper(CASE
    WHEN upper(pa.unit_num) = '08158'
     AND upper(pa.sub_unit_num) = '00005' THEN '08165'
    WHEN upper(pa.unit_num) = '00031'
     AND upper(pa.sub_unit_num) = '00002' THEN '00584'
    ELSE pa.unit_num
  END), upper(pa.admission_patient_type_code), upper(pa.patient_type_code_pos1), upper(pt.admission_prev_pat_type_code), 7, upper(php.physician_name), 9, upper(phr.physician_name), upper(CASE
    WHEN upper(substr(php.physician_name, 1, 4)) = 'NO P' THEN 'NO PCP'
    WHEN upper(substr(php.physician_name, 1, 12)) = 'PHYSICIAN NO' THEN 'NO PCP'
    WHEN upper(substr(php.physician_name, 1, 8)) = 'SELF REF' THEN 'NO PCP'
    WHEN upper(substr(php.physician_name, 1, 13)) = 'REFERRED SELF' THEN 'NO PCP'
    WHEN upper(substr(php.physician_name, 1, 8)) = 'DOES NOT' THEN 'DNK'
    WHEN upper(substr(php.physician_name, 1, 9)) = 'DOES_NOT' THEN 'DNK'
    WHEN upper(substr(php.physician_name, 1, 13)) = 'PROVIDER UNKN' THEN 'DNK'
    WHEN upper(substr(php.physician_name, 1, 6)) = 'URGENT' THEN '.URGENT'
    WHEN php.facility_physician_num = 0 THEN 'BLANK'
    WHEN php.facility_physician_num = 9999 THEN 'IDENTIFIED'
    WHEN php.facility_physician_num <> 9999 THEN 'IDENTIFIED'
    ELSE 'BLANK'
  END), upper(CASE
    WHEN upper(substr(phr.physician_name, 1, 4)) = 'NO P' THEN 'DNK'
    WHEN upper(substr(phr.physician_name, 1, 8)) = 'SELF REF' THEN 'SELF'
    WHEN upper(substr(phr.physician_name, 1, 13)) = 'REFERRED SELF' THEN 'SELF'
    WHEN upper(substr(phr.physician_name, 1, 8)) = 'DOES NOT' THEN 'DNK'
    WHEN upper(substr(phr.physician_name, 1, 13)) = 'PROVIDER UNKN' THEN 'DNK'
    WHEN upper(substr(phr.physician_name, 1, 12)) = 'PHYSICIAN NO' THEN 'DNK'
    WHEN upper(substr(phr.physician_name, 1, 6)) = 'URGENT' THEN '.URGENT'
    WHEN phr.facility_physician_num = 0 THEN 'BLANK'
    WHEN phr.facility_physician_num = 9999 THEN 'IDENTIFIED'
    WHEN phr.facility_physician_num <> 9999 THEN 'IDENTIFIED'
    ELSE 'BLANK'
  END), upper(CASE
    WHEN upper(substr(pt.admission_prev_pat_type_code, 1, 1)) = 'E'
     AND upper(pa.patient_type_code_pos1) = 'I' THEN 'ED ADMITS'
    WHEN upper(pa.patient_type_code_pos1) = 'I' THEN 'NON-ED ADMITS'
    WHEN upper(pa.patient_type_code_pos1) = 'E' THEN 'ED TREAT & RELEASE'
    WHEN upper(pa.patient_type_code_pos1) = 'O' THEN 'OP TREAT & RELEASE'
    WHEN upper(pa.patient_type_code_pos1) = 'S' THEN 'SDC TREAT & RELEASE'
    ELSE CAST(NULL as STRING)
  END), 14, upper(pa.coid), 16
;
