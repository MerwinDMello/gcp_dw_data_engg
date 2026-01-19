-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fm_pcp_case_cnt.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fm_pcp_case_cnt AS SELECT
    fd.customer_code,
    fd.ssc_code,
    CASE
      WHEN pa.unit_num = '08158'
       AND pa.sub_unit_num = '00005' THEN '08165'
      WHEN pa.unit_num = '00031'
       AND pa.sub_unit_num = '00002' THEN '00584'
      ELSE pa.unit_num
    END AS unit_num,
    pa.admission_patient_type_code,
    pa.patient_type_code_pos1,
    pt.admission_prev_pat_type_code,
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
      WHEN substr(pt.admission_prev_pat_type_code, 1, 1) = 'E'
       AND pa.patient_type_code_pos1 = 'I' THEN 'ED ADMITS'
      WHEN pa.patient_type_code_pos1 = 'I' THEN 'NON-ED ADMITS'
      WHEN pa.patient_type_code_pos1 = 'E' THEN 'ED TREAT & RELEASE'
      WHEN pa.patient_type_code_pos1 = 'O' THEN 'OP TREAT & RELEASE'
      WHEN pa.patient_type_code_pos1 = 'S' THEN 'SDC TREAT & RELEASE'
      ELSE CAST(NULL as STRING)
    END) AS type_of_admission,
    pa.admission_date,
    pa.coid,
    pa.pat_acct_num,
    count(pa.pat_acct_num) AS number_of_cases
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient AS pa
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_organization AS fd ON fd.unit_num = CASE
      WHEN pa.unit_num = '08158'
       AND pa.sub_unit_num = '00005' THEN '08165'
      WHEN pa.unit_num = '00031'
       AND pa.sub_unit_num = '00002' THEN '00584'
      ELSE pa.unit_num
    END
     AND fd.customer_code IN(
      'ARH', 'HCA'
    )
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.admission_patient_type_pf AS pt ON pt.patient_dw_id = pa.patient_dw_id
     AND pt.eff_to_date = DATE '9999-12-31'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf AS php ON php.patient_dw_id = pa.patient_dw_id
     AND php.role_type_code = 'PCP'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf AS phr ON phr.patient_dw_id = pa.patient_dw_id
     AND phr.role_type_code = 'REF'
  WHERE pa.admission_date BETWEEN date_add(date_trunc(current_date('US/Central'), MONTH), interval -1 MONTH) AND date_sub(date_add(date_trunc(current_date('US/Central'), MONTH), interval 0 MONTH), interval 1 DAY)
   AND pt.admission_patient_type_code NOT IN(
    'OP', 'EP', 'IP', 'IB'
  )
   AND pa.patient_type_code_pos1 IN(
    'I', 'E', 'O', 'S'
  )
  GROUP BY 1, 2, 3, 4, 5, 6, 7, upper(php.physician_name), 9, upper(phr.physician_name), upper(CASE
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
    WHEN substr(pt.admission_prev_pat_type_code, 1, 1) = 'E'
     AND pa.patient_type_code_pos1 = 'I' THEN 'ED ADMITS'
    WHEN pa.patient_type_code_pos1 = 'I' THEN 'NON-ED ADMITS'
    WHEN pa.patient_type_code_pos1 = 'E' THEN 'ED TREAT & RELEASE'
    WHEN pa.patient_type_code_pos1 = 'O' THEN 'OP TREAT & RELEASE'
    WHEN pa.patient_type_code_pos1 = 'S' THEN 'SDC TREAT & RELEASE'
    ELSE CAST(NULL as STRING)
  END), 14, 15, 16
;
