-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_pbs_j_pf_fact_pat_fc_level_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(trim(format('%20d', count(x.unit_num_sid))), trim(CAST(sum(x.cash_receipt_amt) AS STRING)), trim(regexp_replace(format('%#17.2f', CAST(ROUND(sum(x.gross_revenue_amt), 2, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3'))) AS source_string
FROM
  (SELECT max(z.unit_num_sid) AS unit_num_sid,
          z.patient_financial_class_sid,
          z.patient_type_sid,
          z.scenario_sid,
          max(z.time_sid) AS time_sid,
          z.source_sid,
          max(z.company_code) AS company_code,
          max(z.coid) AS coid,
          sum(z.cash_receipt_amt) AS cash_receipt_amt,
          sum(z.gross_revenue_amt) AS gross_revenue_amt
   FROM
     (SELECT max(sar.unit_num) AS unit_num_sid,
             CASE
                 WHEN sar.financial_class_code = 999 THEN 23
                 ELSE epf.patient_financial_class_sid
             END AS patient_financial_class_sid,
             1 AS scenario_sid,
             max(sar.rptg_period) AS time_sid,
             1 AS source_sid,
             CASE
                 WHEN upper(sar.derived_patient_type_code) = 'NA' THEN 40
                 ELSE pt.patient_type_sid
             END AS patient_type_sid,
             max(sar.company_code) AS company_code,
             max(sar.coid) AS coid,
             sum(sar.cash_amt) AS cash_receipt_amt,
             sum(NUMERIC '0.000') AS gross_revenue_amt
      FROM `hca-hin-dev-cur-parallon`.auth_base_views.smry_ar_cash_collections AS sar
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.eis_patient_fin_class_dim AS epf ON CASE substr(epf.patient_financial_class_member, 5, 2)
                                                                                                         WHEN '' THEN 0.0
                                                                                                         ELSE CAST(substr(epf.patient_financial_class_member, 5, 2) AS FLOAT64)
                                                                                                     END = sar.financial_class_code
      AND upper(epf.patient_financial_class_member) <> 'NO_FC'
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.eis_patient_type_dim AS pt ON upper(pt.patient_type_member) = upper(concat('PT_', CASE
                                                                                                                                                       WHEN upper(trim(sar.derived_patient_type_code)) = 'IE' THEN 'E'
                                                                                                                                                       ELSE sar.derived_patient_type_code
                                                                                                                                                   END))
      AND upper(substr(pt.patient_type_gen02, 1, 2)) <> 'MC'
      WHERE sar.payor_dw_id <> 999
        AND upper(sar.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
      GROUP BY upper(sar.unit_num),
               2,
               3,
               upper(sar.rptg_period),
               5,
               6,
               upper(sar.company_code),
               upper(sar.coid)
      UNION DISTINCT SELECT max(fd.unit_num) AS unit_num_sid,
                            epf.patient_financial_class_sid,
                            1 AS scenario_sid,
                            max(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) AS time_sid,
                            1 AS source_sid,
                            CASE upper(rd.data_type_text)
                                WHEN 'IP AMOUNT' THEN 21
                                WHEN 'OP AMOUNT' THEN 27
                            END AS patient_type_sid,
                            max(fd.company_code) AS company_code,
                            max(rd.coid) AS coid,
                            sum(NUMERIC '0.000') AS cash_receipt_amt,
                            sum(rd.month_minus_1_amt) AS gross_revenue_amt
      FROM `hca-hin-dev-cur-parallon`.auth_base_views.revstats_department AS rd
      INNER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.facility_dimension AS fd ON upper(fd.coid) = upper(rd.coid)
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.auth_base_views.eis_patient_fin_class_dim AS epf ON CASE substr(epf.patient_financial_class_member, 5, 2)
                                                                                                         WHEN '' THEN 0.0
                                                                                                         ELSE CAST(substr(epf.patient_financial_class_member, 5, 2) AS FLOAT64)
                                                                                                     END = rd.financial_class_code
      AND upper(epf.patient_financial_class_member) <> 'NO_FC'
      WHERE upper(rd.data_type_text) IN('IP AMOUNT',
                                        'OP AMOUNT')
        AND upper(rd.sub_unit_num) <> '00000'
        AND (upper(fd.company_code) = 'H'
             OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
      GROUP BY upper(fd.unit_num),
               2,
               3,
               upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))),
               5,
               6,
               upper(fd.company_code),
               upper(rd.coid)) AS z
   GROUP BY upper(z.unit_num_sid),
            2,
            3,
            4,
            upper(z.time_sid),
            6,
            upper(z.company_code),
            upper(z.coid)) AS x 