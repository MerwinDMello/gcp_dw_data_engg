-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_pbs_j_pf_fact_pat_upfrntcol_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- LOCKING TABLE Edwpf.AR_Transaction FOR ACCESS
-- LOCKING TABLE Edwfs.Fact_Facility FOR ACCESS
-- LOCKING TABLE edwpf.Admission FOR ACCESS
-- LOCKING TABLE edwpf.Admission_Discharge FOR ACCESS
-- LOCKING TABLE edwpf.Registration_Account_Status FOR ACCESS

SELECT coalesce(CAST(k.sum1 AS STRING), '0') AS source_string
FROM
  (SELECT date_sub(date_add(CAST(trim(concat(format_date('%Y-%m', art.ar_transaction_effective_date), '-01')) AS DATE), interval 1 MONTH), interval 1 DAY) AS effective_date,
          sum(art.ar_transaction_amt) AS sum1
   FROM `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction AS art
   INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.fact_facility AS ff ON upper(ff.coid) = upper(art.coid)
   AND upper(ff.company_code) = upper(art.company_code)
   INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.registration_account_status AS sts ON art.patient_dw_id = sts.patient_dw_id
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.admission_discharge AS ad ON sts.patient_dw_id = ad.patient_dw_id
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_ar_patient_level AS fp ON art.pat_acct_num = fp.patient_sid
   AND upper(art.company_code) = upper(fp.company_code)
   AND upper(art.coid) = upper(fp.coid)
   AND CASE format_date('%Y%m', art.ar_transaction_effective_date)
           WHEN '' THEN 0
           ELSE CAST(format_date('%Y%m', art.ar_transaction_effective_date) AS INT64)
       END = fp.date_sid
   AND fp.iplan_insurance_order_num = 0
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.admission AS adm ON adm.patient_dw_id = art.patient_dw_id
   LEFT OUTER JOIN
     (SELECT rcom_payor_dimension_eom.payor_dw_id,
             rcom_payor_dimension_eom.payor_id,
             rcom_payor_dimension_eom.eff_from_date,
             rcom_payor_dimension_eom.eff_to_date
      FROM `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom) AS rpdeb ON art.payor_dw_id = rpdeb.payor_dw_id
   AND adm.admission_date BETWEEN rpdeb.eff_from_date AND rpdeb.eff_to_date
   LEFT OUTER JOIN
     (SELECT rcom_payor_dimension_eom.payor_dw_id,
             rcom_payor_dimension_eom.payor_id,
             rcom_payor_dimension_eom.eff_from_date
      FROM `hca-hin-dev-cur-parallon`.edwpbs_base_views.rcom_payor_dimension_eom) AS rpde ON art.payor_dw_id = rpde.payor_dw_id
   AND rpde.eff_from_date IS NULL
   WHERE sts.account_status_date =
       (SELECT max(stsm.account_status_date)
        FROM `hca-hin-dev-cur-parallon`.auth_base_views.registration_account_status AS stsm
        WHERE stsm.patient_dw_id = sts.patient_dw_id
          AND stsm.account_status_date <= date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY) )
     AND upper(art.transaction_type_code) = '1'
     AND art.iplan_id = 0
     AND (upper(art.company_code) = 'H'
          OR upper(substr(trim(ff.company_code_operations), 1, 1)) = 'Y')
     AND art.ar_transaction_enter_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY)
     AND art.ar_transaction_effective_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_sub(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 1 DAY)
     AND upper(sts.account_status_code) IN('AR',
                                           'UB',
                                           'CA')
     AND (ad.discharge_date IS NULL
          OR art.ar_transaction_effective_date <= date_add(ad.discharge_date, interval 2 DAY))
   GROUP BY 1) AS k 