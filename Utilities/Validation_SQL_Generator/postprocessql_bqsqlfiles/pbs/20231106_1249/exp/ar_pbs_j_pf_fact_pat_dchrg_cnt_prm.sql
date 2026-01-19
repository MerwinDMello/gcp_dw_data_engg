-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_pbs_j_pf_fact_pat_dchrg_cnt_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- Locking table edwpf_base_views.FACT_PATIENT for access
-- Locking table edwpf_base_views.Registration_Service for access
-- Locking table edwpf_base_views.Ref_Service for access
-- Locking table EDWPF_Staging.Pass_Current for access

SELECT concat(trim(format('%20d', coalesce(count(k.pat_acct_num), 0))), trim(CAST(sum(CAST(k.discharge_to_billing_day_cnt AS NUMERIC)) AS STRING))) AS source_string
FROM
  (SELECT a.pat_acct_num,
          CASE format_date('%Y%m', date_sub(current_date('US/Central'), interval 1 MONTH))
              WHEN '' THEN 0
              ELSE CAST(format_date('%Y%m', date_sub(current_date('US/Central'), interval 1 MONTH)) AS INT64)
          END AS date_sid, -- cast((a.final_bill_date-a.discharge_date) as decimal(20,0)) as Discharge_To_Billing_Day_Cnt,
 date_diff(a.final_bill_date, a.discharge_date, DAY) AS discharge_to_billing_day_cnt,
 max(a.company_code) AS company_code,
 max(a.coid) AS coid,
 CASE ff.unit_num
     WHEN '' THEN 0
     ELSE CAST(ff.unit_num AS INT64)
 END AS unitnum,
 CASE
     WHEN fp.account_type_sid IS NULL THEN 1
     ELSE fp.account_type_sid
 END AS account_type_sid,
 CASE
     WHEN fp.account_status_sid IS NULL THEN 10
     ELSE fp.account_status_sid
 END AS account_status_sid,
 CASE
     WHEN fp.age_month_sid IS NULL THEN 1
     ELSE fp.age_month_sid
 END AS age_month_sid,
 CASE
     WHEN fp.patient_financial_class_sid IS NULL THEN 23
     ELSE fp.patient_financial_class_sid
 END AS patient_financial_class_sid,
 CASE
     WHEN fp.patient_type_sid IS NULL THEN 40
     ELSE fp.patient_type_sid
 END AS patient_type_sid,
 CASE
     WHEN fp.collection_agency_sid IS NULL THEN 1
     ELSE fp.collection_agency_sid
 END AS collection_agency_sid,
 CASE
     WHEN fp.payor_sid IS NULL THEN 1
     ELSE fp.payor_sid
 END AS payor_sid,
 CASE
     WHEN fp.payor_financial_class_sid IS NULL THEN 24
     ELSE fp.payor_financial_class_sid
 END AS payor_financial_class_sid,
 CASE
     WHEN fp.product_sid IS NULL THEN 22
     ELSE fp.product_sid
 END AS product_sid,
 CASE
     WHEN fp.contract_sid IS NULL THEN 1
     ELSE fp.contract_sid
 END AS contract_sid,
 CASE
     WHEN fp.scenario_sid IS NULL THEN 1
     ELSE fp.scenario_sid
 END AS scenario_sid,
 CASE
     WHEN fp.source_sid IS NULL THEN 1
     ELSE fp.source_sid
 END AS source_sid,
 0 AS iplan_insurance_order_num,
 1 AS billed_patient_cnt
   FROM `hca-hin-dev-cur-parallon`.auth_base_views.fact_patient AS a
   INNER JOIN
     (SELECT y.patient_dw_id,
             max(y.service_code_start_date) AS service_code_start_date
      FROM `hca-hin-dev-cur-parallon`.auth_base_views.registration_service AS y
      GROUP BY 1) AS z ON z.patient_dw_id = a.patient_dw_id
   INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.registration_service AS b ON b.patient_dw_id = a.patient_dw_id
   AND b.service_code_start_date = z.service_code_start_date
   INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.ref_service AS c ON upper(b.coid) = upper(c.coid)
   AND upper(b.service_code) = upper(c.service_code)
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_staging.pass_current AS pc ON a.patient_dw_id = pc.patient_dw_id
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_patient_level AS fp ON a.pat_acct_num = fp.patient_sid
   AND upper(fp.company_code) = upper(a.company_code)
   AND upper(fp.coid) = upper(a.coid)
   AND fp.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                         WHEN '' THEN 0
                         ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                     END
   AND fp.iplan_insurance_order_num = 0
   AND fp.age_month_sid = 20
   INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.fact_facility AS ff ON upper(ff.coid) = upper(a.coid)
   AND upper(ff.company_code) = upper(a.company_code)
   WHERE (upper(a.company_code) = 'H'
          OR upper(substr(trim(ff.company_code_operations), 1, 1)) = 'Y')
     AND CASE
             WHEN pc.eom_total_chgs IS NOT NULL
                  AND CASE pc.eom_total_chgs
                          WHEN '' THEN NUMERIC '0'
                          ELSE CAST(pc.eom_total_chgs AS NUMERIC)
                      END <> 0 THEN ROUND(CASE pc.eom_total_chgs
                                              WHEN '' THEN NUMERIC '0'
                                              ELSE CAST(pc.eom_total_chgs AS NUMERIC)
                                          END / 100, 3, 'ROUND_HALF_EVEN')
             ELSE a.total_billed_charges
         END <> 0
     AND upper(a.patient_type_code_pos1) = 'I'
     AND upper(c.service_exempt_flag) = 'N'
     AND (a.discharge_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_sub(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 1 DAY)
          AND a.final_bill_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY)
          OR a.discharge_date <= date_sub(date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH), interval 1 DAY)
          AND a.final_bill_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -1 MONTH)), '-01')) AS DATE), interval 4 DAY) AND date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY))
     AND b.service_code_start_date <= a.discharge_date
   GROUP BY 1,
            2,
            3,
            upper(a.company_code),
            upper(a.coid),
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20) AS k 