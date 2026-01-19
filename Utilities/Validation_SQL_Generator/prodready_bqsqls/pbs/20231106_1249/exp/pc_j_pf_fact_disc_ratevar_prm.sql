-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pc_j_pf_fact_disc_ratevar_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(coalesce(CAST(sum(x.eor_gross_reimbursement_amt) AS STRING), '0'), coalesce(CAST(sum(x.eor_contractual_allowance_amt) AS STRING), '0'), coalesce(CAST(sum(x.eor_insurance_payment_amt) AS STRING), '0')) AS source_string
FROM
  (SELECT max(c.company_code) AS company_code,
          max(c.coid) AS coid,
          c.patient_dw_id,
          c.payor_dw_id,
          max(c.log_id) AS log_id,
          c.eor_log_date,
          c.log_sequence_num,
          finalbilldate,
          ad.discharge_date,
          d.financial_class_code,
          max(ff.unit_num) AS unit_num,
          max(CASE
                  WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                       AND upper(frl.ip_log_format_code) = 'O' THEN '0124'
                  WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                       AND upper(frl.ip_log_format_code) = 'M' THEN '0125'
                  WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                       AND upper(frl.op_log_format_code) = 'O' THEN '0124'
                  WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                       AND upper(frl.op_log_format_code) = 'M' THEN '0125'
                  WHEN upper(d.source_system_code) = 'N'
                       AND d.financial_class_code IN(1, 2, 3, 6, 9) THEN '0125'
                  WHEN upper(d.source_system_code) = 'N' THEN '0124'
                  ELSE '0000'
              END) AS datasourcecode,
          max(apt.admission_patient_type_code) AS admission_patient_type_code,
          sum(d.eor_gross_reimbursement_amt) AS eor_gross_reimbursement_amt,
          sum(d.eor_contractual_allowance_amt) AS eor_contractual_allowance_amt,
          sum(d.eor_insurance_payment_amt) AS eor_insurance_payment_amt,
          d.ar_bill_thru_date,
          max(d.source_system_code) AS source_system_code,
          max(coalesce(date_diff(date_sub(current_date('US/Central'), interval extract(DAY
                                                                                       FROM current_date('US/Central')) DAY), ad.discharge_date, DAY), 0)) AS disch_days
   FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS d
   INNER JOIN
     (SELECT max(k.company_code) AS company_code,
             max(k.coid) AS coid,
             k.patient_dw_id,
             k.payor_dw_id,
             max(k.log_id) AS log_id,
             k.ar_bill_thru_date,
             k.iplan_insurance_order_num,
             k.final_bill_date,
             k.eor_log_date,
             k.log_sequence_num,
             min(k.eff_from_date) AS eff_from_date
      FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS k
      INNER JOIN
        (SELECT max(h.company_code) AS company_code,
                max(h.coid) AS coid,
                h.patient_dw_id,
                h.payor_dw_id,
                max(h.log_id) AS log_id,
                h.ar_bill_thru_date,
                h.iplan_insurance_order_num,
                h.final_bill_date,
                h.eor_log_date,
                min(h.log_sequence_num) AS log_sequence_num
         FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS h
         INNER JOIN
           (SELECT max(a.company_code) AS company_code,
                   max(a.coid) AS coid,
                   a.patient_dw_id,
                   a.payor_dw_id,
                   max(a.log_id) AS log_id,
                   a.ar_bill_thru_date,
                   a.iplan_insurance_order_num,
                   a.final_bill_date,
                   min(a.eor_log_date) AS eor_log_date
            FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS a
            INNER JOIN
              (SELECT max(b.company_code) AS company_code,
                      max(b.coid) AS coid,
                      b.patient_dw_id,
                      b.payor_dw_id,
                      max(b.log_id) AS log_id,
                      b.ar_bill_thru_date,
                      b.final_bill_date,
                      b.iplan_insurance_order_num
               FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS b
               WHERE upper(b.eor_reversal_code) = ' '
                 AND b.iplan_insurance_order_num = 1
               GROUP BY upper(b.company_code),
                        upper(b.coid),
                        3,
                        4,
                        upper(b.log_id),
                        6,
                        7,
                        8) AS z ON upper(a.company_code) = upper(z.company_code)
            AND upper(a.coid) = upper(z.coid)
            AND a.patient_dw_id = z.patient_dw_id
            AND a.payor_dw_id = z.payor_dw_id
            AND upper(a.log_id) = upper(z.log_id)
            AND a.ar_bill_thru_date = z.ar_bill_thru_date
            AND a.final_bill_date = z.final_bill_date
            AND a.iplan_insurance_order_num = z.iplan_insurance_order_num
            GROUP BY upper(a.company_code),
                     upper(a.coid),
                     3,
                     4,
                     upper(a.log_id),
                     6,
                     7,
                     8) AS g ON upper(h.company_code) = upper(g.company_code)
         AND upper(h.coid) = upper(g.coid)
         AND h.patient_dw_id = g.patient_dw_id
         AND h.payor_dw_id = g.payor_dw_id
         AND upper(h.log_id) = upper(g.log_id)
         AND h.ar_bill_thru_date = g.ar_bill_thru_date
         AND h.final_bill_date = g.final_bill_date
         AND h.iplan_insurance_order_num = g.iplan_insurance_order_num
         AND h.eor_log_date = g.eor_log_date
         GROUP BY upper(h.company_code),
                  upper(h.coid),
                  3,
                  4,
                  upper(h.log_id),
                  6,
                  7,
                  8,
                  9) AS j ON upper(k.company_code) = upper(j.company_code)
      AND upper(k.coid) = upper(j.coid)
      AND k.patient_dw_id = j.patient_dw_id
      AND k.payor_dw_id = j.payor_dw_id
      AND upper(k.log_id) = upper(j.log_id)
      AND k.ar_bill_thru_date = j.ar_bill_thru_date
      AND k.final_bill_date = j.final_bill_date
      AND k.iplan_insurance_order_num = j.iplan_insurance_order_num
      AND k.eor_log_date = j.eor_log_date
      AND k.log_sequence_num = j.log_sequence_num
      GROUP BY upper(k.company_code),
               upper(k.coid),
               3,
               4,
               upper(k.log_id),
               6,
               7,
               8,
               9,
               10) AS c ON upper(d.company_code) = upper(c.company_code)
   AND upper(d.coid) = upper(c.coid)
   AND d.patient_dw_id = c.patient_dw_id
   AND d.payor_dw_id = c.payor_dw_id
   AND upper(d.log_id) = upper(c.log_id)
   AND d.final_bill_date = c.final_bill_date
   AND d.ar_bill_thru_date = c.ar_bill_thru_date
   AND d.eor_log_date = c.eor_log_date
   AND d.log_sequence_num = c.log_sequence_num
   AND d.eff_from_date = c.eff_from_date
   AND d.iplan_insurance_order_num = c.iplan_insurance_order_num
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.admission_discharge AS ad ON ad.patient_dw_id = d.patient_dw_id
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility AS ff ON upper(ff.coid) = upper(d.coid)
   AND upper(ff.company_code) = upper(d.company_code)
   INNER JOIN
     (SELECT pt.patient_dw_id,
             max(pt.eff_from_date) AS eff_from_date
      FROM {{ params.param_auth_base_views_dataset_name }}.admission_patient_type AS pt
      GROUP BY 1) AS atp ON atp.patient_dw_id = d.patient_dw_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.admission_patient_type AS apt ON apt.patient_dw_id = d.patient_dw_id
   AND apt.eff_from_date = atp.eff_from_date
   LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_reimbursement_log AS frl ON upper(d.company_code) = upper(frl.company_code)
   AND upper(d.coid) = upper(frl.coid)
   AND upper(d.log_id) = upper(frl.log_id)
   AND upper(frl.log_reports_ind) = 'Y'
   AND frl.inactive_date > date_sub(CAST(trim(concat(format_date('%Y-%m', current_date('US/Central')), '-01')) AS DATE), interval 1 DAY)
   AND upper(d.source_system_code) = 'P'
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN d.final_bill_date = DATE '0001-01-01' THEN d.ar_bill_thru_date
                                ELSE d.final_bill_date
                            END ]) AS finalbilldate
   WHERE upper(d.log_id) <> 'MSP'
     AND finalbilldate BETWEEN CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -3 MONTH)), '-01')) AS DATE) AND date_sub(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -1 MONTH)), '-01')) AS DATE), interval 1 DAY)
     AND upper(CASE
                   WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                        AND upper(frl.ip_log_format_code) = 'O' THEN '0124'
                   WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                        AND upper(frl.ip_log_format_code) = 'M' THEN '0125'
                   WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                        AND upper(frl.op_log_format_code) = 'O' THEN '0124'
                   WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                        AND upper(frl.op_log_format_code) = 'M' THEN '0125'
                   WHEN upper(d.source_system_code) = 'N'
                        AND d.financial_class_code IN(1, 2, 3, 6, 9) THEN '0125'
                   WHEN upper(d.source_system_code) = 'N' THEN '0124'
                   ELSE '0000'
               END) <> '0000'
     AND upper(d.coid) NOT IN
       (SELECT upper(parallon_client_detail.coid) AS coid
        FROM {{ params.param_pbs_core_dataset_name }}.parallon_client_detail
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')
        WHERE upper(parallon_client_detail.company_code) = 'CHP' )
   GROUP BY upper(c.company_code),
            upper(c.coid),
            3,
            4,
            upper(c.log_id),
            6,
            7,
            8,
            9,
            10,
            upper(ff.unit_num),
            upper(CASE
                      WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                           AND upper(frl.ip_log_format_code) = 'O' THEN '0124'
                      WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) = 'I'
                           AND upper(frl.ip_log_format_code) = 'M' THEN '0125'
                      WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                           AND upper(frl.op_log_format_code) = 'O' THEN '0124'
                      WHEN upper(substr(apt.admission_patient_type_code, 1, 1)) <> 'I'
                           AND upper(frl.op_log_format_code) = 'M' THEN '0125'
                      WHEN upper(d.source_system_code) = 'N'
                           AND d.financial_class_code IN(1, 2, 3, 6, 9) THEN '0125'
                      WHEN upper(d.source_system_code) = 'N' THEN '0124'
                      ELSE '0000'
                  END),
            upper(apt.admission_patient_type_code),
            17,
            upper(d.source_system_code)) AS x