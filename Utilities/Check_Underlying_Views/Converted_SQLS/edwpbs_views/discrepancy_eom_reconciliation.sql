-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/discrepancy_eom_reconciliation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.discrepancy_eom_reconciliation AS SELECT
    max(rd.pas_current_name) AS pas_current_name,
    max(rd.coid) AS coid,
    max(fd.unit_num) AS unit_num,
    max(rd.type_code) AS type_code,
    sum(rd.underpymtamt) AS ending_amt
  FROM
    (
      SELECT DISTINCT
          fd_0.pas_current_name,
          rd_0.iplan_id,
          rd_0.coid,
          rd_0.log_id,
          rd_0.payor_dw_id,
          rd_0.iplan_insurance_order_num,
          rd_0.pat_acct_num,
          rd_0.ar_bill_thru_date,
          rd_0.discrepancy_origination_date,
          'MgdCare' AS type_code,
          CASE
            WHEN upper(rd_0.discrepancy_reason_code_1) IN(
              '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_2) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_3) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_4) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            ) THEN CAST(0 as BIGNUMERIC)
            ELSE abs(rd_0.var_gross_reimbursement_amt)
          END AS varpymt,
          CASE
            WHEN CASE
               rd_0.data_source_code
              WHEN '' THEN 0.0
              ELSE CAST(rd_0.data_source_code as FLOAT64)
            END = 125 THEN abs(rd_0.var_insurance_payment_amt)
            ELSE CAST(0 as BIGNUMERIC)
          END AS lg_mcr_amt,
          CASE
            WHEN CASE
               rd_0.data_source_code
              WHEN '' THEN 0.0
              ELSE CAST(rd_0.data_source_code as FLOAT64)
            END = 125 THEN abs(rd_0.var_insurance_payment_amt)
            ELSE CAST(0 as BIGNUMERIC)
          END + CASE
            WHEN upper(rd_0.discrepancy_reason_code_1) IN(
              '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_2) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_3) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_4) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            ) THEN CAST(0 as BIGNUMERIC)
            ELSE abs(rd_0.var_gross_reimbursement_amt)
          END AS underpymtamt
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.reimbursement_discrepancy_eom AS rd_0
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_dimension_eom AS fd_0 ON upper(fd_0.coid) = upper(rd_0.coid)
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.admission_discharge AS ad ON ad.patient_dw_id = rd_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_year_dim AS b ON CASE
             fd_0.unit_num
            WHEN '' THEN 0
            ELSE CAST(fd_0.unit_num as INT64)
          END = b.unit_num_sid
           AND b.source_sid = CASE
            WHEN upper(rd_0.data_source_code) = '0124'
             AND upper(rd_0.source_system_code) = 'P' THEN 6
            WHEN upper(rd_0.data_source_code) = '0124'
             AND upper(rd_0.source_system_code) = 'N' THEN 11
            ELSE 0
          END
           AND CASE
            WHEN rd_0.ar_bill_thru_date IN(
              DATE '0001-01-01', DATE '1800-01-01'
            ) THEN ad.discharge_date
            ELSE rd_0.ar_bill_thru_date
          END BETWEEN b.year_beg_date AND b.year_end_date
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON CASE
             fd_0.unit_num
            WHEN '' THEN 0
            ELSE CAST(fd_0.unit_num as INT64)
          END = p.unit_num_sid
           AND p.source_sid = CASE
            WHEN upper(rd_0.data_source_code) = '0124'
             AND upper(rd_0.source_system_code) = 'P' THEN 6
            WHEN upper(rd_0.data_source_code) = '0124'
             AND upper(rd_0.source_system_code) = 'N' THEN 11
            ELSE 0
          END
           AND rd_0.discrepancy_origination_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS eor ON upper(rd_0.company_code) = upper(eor.company_code)
           AND upper(rd_0.coid) = upper(eor.coid)
           AND rd_0.patient_dw_id = eor.patient_dw_id
           AND rd_0.payor_dw_id = eor.payor_dw_id
           AND rd_0.iplan_insurance_order_num = eor.iplan_insurance_order_num
           AND rd_0.eor_log_date = eor.eor_log_date
           AND upper(rd_0.log_id) = upper(eor.log_id)
           AND rd_0.log_sequence_num = eor.log_sequence_num
        WHERE eor.eff_from_date = (
          SELECT
              max(er.eff_from_date)
            FROM
              `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS er
            WHERE upper(rd_0.company_code) = upper(er.company_code)
             AND upper(rd_0.coid) = upper(er.coid)
             AND rd_0.patient_dw_id = er.patient_dw_id
             AND rd_0.payor_dw_id = er.payor_dw_id
             AND rd_0.iplan_insurance_order_num = er.iplan_insurance_order_num
             AND rd_0.eor_log_date = er.eor_log_date
             AND upper(rd_0.log_id) = upper(er.log_id)
             AND rd_0.log_sequence_num = er.log_sequence_num
             AND er.eff_from_date <= date_sub(CAST(concat(format_date('%Y-%m', current_date('US/Central')), '-01') as DATE), interval 1 DAY)
        )
         AND upper(rd_0.data_source_code) = '0124'
         AND upper(rd_0.source_system_code) IN(
          'N', 'P'
        )
      UNION ALL
      SELECT
          max(fd_0.pas_current_name) AS pas_current_name,
          rd_0.iplan_id,
          max(rd_0.coid) AS coid,
          max(rd_0.log_id) AS log_id,
          rd_0.payor_dw_id,
          rd_0.iplan_insurance_order_num,
          rd_0.pat_acct_num,
          rd_0.ar_bill_thru_date,
          rd_0.discrepancy_origination_date,
          'Medicar' AS type_code,
          sum(CASE
            WHEN upper(rd_0.discrepancy_reason_code_1) IN(
              '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_2) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_3) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_4) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            ) THEN CAST(0 as BIGNUMERIC)
            ELSE abs(rd_0.var_gross_reimbursement_amt)
          END) AS varpymt,
          sum(CASE
            WHEN CASE
               rd_0.data_source_code
              WHEN '' THEN 0.0
              ELSE CAST(rd_0.data_source_code as FLOAT64)
            END = 125 THEN abs(rd_0.var_insurance_payment_amt)
            ELSE CAST(0 as BIGNUMERIC)
          END) AS lg_mcr_amt,
          sum(CASE
            WHEN CASE
               rd_0.data_source_code
              WHEN '' THEN 0.0
              ELSE CAST(rd_0.data_source_code as FLOAT64)
            END = 125 THEN abs(rd_0.var_insurance_payment_amt)
            ELSE CAST(0 as BIGNUMERIC)
          END) + sum(CASE
            WHEN upper(rd_0.discrepancy_reason_code_1) IN(
              '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_2) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_3) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            )
             OR upper(rd_0.discrepancy_reason_code_4) IN(
              '801', '712', '713', '801', '865', '866', '867', '868', '869'
            ) THEN CAST(0 as BIGNUMERIC)
            ELSE abs(rd_0.var_gross_reimbursement_amt)
          END) AS underpymtamt
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.reimbursement_discrepancy_eom AS rd_0
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_dimension_eom AS fd_0 ON upper(fd_0.coid) = upper(rd_0.coid)
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.admission_discharge AS ad ON ad.patient_dw_id = rd_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON CASE
             fd_0.unit_num
            WHEN '' THEN 0
            ELSE CAST(fd_0.unit_num as INT64)
          END = b.unit_num_sid
           AND b.source_sid = CASE
            WHEN upper(rd_0.data_source_code) = '0124' THEN 6
            ELSE 7
          END
           AND CASE
            WHEN rd_0.ar_bill_thru_date IN(
              DATE '0001-01-01', DATE '1800-01-01'
            ) THEN ad.discharge_date
            ELSE rd_0.ar_bill_thru_date
          END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_year_dim AS p ON CASE
             fd_0.unit_num
            WHEN '' THEN 0
            ELSE CAST(fd_0.unit_num as INT64)
          END = p.unit_num_sid
           AND p.source_sid = CASE
            WHEN upper(rd_0.data_source_code) = '0124' THEN 6
            ELSE 7
          END
           AND rd_0.discrepancy_origination_date BETWEEN p.year_beg_date AND p.year_end_date
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS eor ON upper(rd_0.company_code) = upper(eor.company_code)
           AND upper(rd_0.coid) = upper(eor.coid)
           AND rd_0.patient_dw_id = eor.patient_dw_id
           AND rd_0.payor_dw_id = eor.payor_dw_id
           AND rd_0.iplan_insurance_order_num = eor.iplan_insurance_order_num
           AND rd_0.eor_log_date = eor.eor_log_date
           AND upper(rd_0.log_id) = upper(eor.log_id)
           AND rd_0.log_sequence_num = eor.log_sequence_num
        WHERE eor.eff_from_date = (
          SELECT
              max(er.eff_from_date)
            FROM
              `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS er
            WHERE upper(rd_0.company_code) = upper(er.company_code)
             AND upper(rd_0.coid) = upper(er.coid)
             AND rd_0.patient_dw_id = er.patient_dw_id
             AND rd_0.payor_dw_id = er.payor_dw_id
             AND rd_0.iplan_insurance_order_num = er.iplan_insurance_order_num
             AND rd_0.eor_log_date = er.eor_log_date
             AND upper(rd_0.log_id) = upper(er.log_id)
             AND rd_0.log_sequence_num = er.log_sequence_num
             AND er.eff_from_date <= date_sub(CAST(concat(format_date('%Y-%m', current_date('US/Central')), '-01') as DATE), interval 1 DAY)
        )
         AND upper(rd_0.data_source_code) = '0125'
         AND upper(rd_0.source_system_code) IN(
          'P'
        )
        GROUP BY upper(fd_0.pas_current_name), 2, upper(rd_0.coid), upper(rd_0.log_id), 5, 6, 7, 8, 9
      UNION ALL
      SELECT
          cw_ss.pas_current_name,
          cw_ss.iplan_id,
          cw_ss.coid,
          cw_ss.log_id,
          cw_ss.payor_dw_id,
          cw_ss.iplan_insurance_order_num,
          cw_ss.pat_acct_num,
          cw_ss.ar_bill_thru_date,
          cw_ss.discrepancy_origination_date,
          substr(cw_ss.type_code, 1, 7) AS type_code,
          cw_ss.varpymt,
          CAST(cw_ss.lg_mcr_amt as BIGNUMERIC) AS lg_mcr_amt,
          cw_ss.underpymtamt
        FROM
          (
            SELECT
                max(fd_0.pas_current_name) AS pas_current_name,
                rd_0.iplan_id,
                max(rd_0.coid) AS coid,
                max(rd_0.log_id) AS log_id,
                rd_0.payor_dw_id,
                rd_0.iplan_insurance_order_num,
                rd_0.pat_acct_num,
                rd_0.ar_bill_thru_date,
                rd_0.discrepancy_origination_date,
                'MC-Con' AS type_code,
                sum(CASE
                  WHEN upper(rd_0.discrepancy_reason_code_1) IN(
                    '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_2) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_3) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_4) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  ) THEN CAST(0 as BIGNUMERIC)
                  ELSE abs(rd_0.var_gross_reimbursement_amt)
                END) AS varpymt,
                sum(CASE
                  WHEN CASE
                     rd_0.data_source_code
                    WHEN '' THEN 0.0
                    ELSE CAST(rd_0.data_source_code as FLOAT64)
                  END = 125 THEN 0
                  ELSE 0
                END) AS lg_mcr_amt,
                sum(CASE
                  WHEN CASE
                     rd_0.data_source_code
                    WHEN '' THEN 0.0
                    ELSE CAST(rd_0.data_source_code as FLOAT64)
                  END = 125 THEN 0
                  ELSE 0
                END) + sum(CASE
                  WHEN upper(rd_0.discrepancy_reason_code_1) IN(
                    '801', '713', '712', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_2) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_3) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  )
                   OR upper(rd_0.discrepancy_reason_code_4) IN(
                    '801', '712', '713', '801', '865', '866', '867', '868', '869'
                  ) THEN CAST(0 as BIGNUMERIC)
                  ELSE abs(rd_0.var_gross_reimbursement_amt)
                END) AS underpymtamt
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.reimbursement_discrepancy_eom AS rd_0
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_dimension_eom AS fd_0 ON upper(fd_0.coid) = upper(rd_0.coid)
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.admission_discharge AS ad ON ad.patient_dw_id = rd_0.patient_dw_id
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS b ON CASE
                   fd_0.unit_num
                  WHEN '' THEN 0
                  ELSE CAST(fd_0.unit_num as INT64)
                END = b.unit_num_sid
                 AND b.source_sid = CASE
                  WHEN upper(rd_0.data_source_code) = '0124' THEN 6
                  ELSE 7
                END
                 AND CASE
                  WHEN rd_0.ar_bill_thru_date IN(
                    DATE '0001-01-01', DATE '1800-01-01'
                  ) THEN ad.discharge_date
                  ELSE rd_0.ar_bill_thru_date
                END BETWEEN b.cost_year_beg_date AND b.cost_year_end_date
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS p ON CASE
                   fd_0.unit_num
                  WHEN '' THEN 0
                  ELSE CAST(fd_0.unit_num as INT64)
                END = p.unit_num_sid
                 AND p.source_sid = CASE
                  WHEN upper(rd_0.data_source_code) = '0124' THEN 6
                  ELSE 7
                END
                 AND rd_0.discrepancy_origination_date BETWEEN p.cost_year_beg_date AND p.cost_year_end_date
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS eor ON upper(rd_0.company_code) = upper(eor.company_code)
                 AND upper(rd_0.coid) = upper(eor.coid)
                 AND rd_0.patient_dw_id = eor.patient_dw_id
                 AND rd_0.payor_dw_id = eor.payor_dw_id
                 AND rd_0.iplan_insurance_order_num = eor.iplan_insurance_order_num
                 AND rd_0.eor_log_date = eor.eor_log_date
                 AND upper(rd_0.log_id) = upper(eor.log_id)
                 AND rd_0.log_sequence_num = eor.log_sequence_num
              WHERE eor.eff_from_date = (
                SELECT
                    max(er.eff_from_date)
                  FROM
                    `hca-hin-dev-cur-parallon`.edwpf_views.explanation_of_reimbursement AS er
                  WHERE upper(rd_0.company_code) = upper(er.company_code)
                   AND upper(rd_0.coid) = upper(er.coid)
                   AND rd_0.patient_dw_id = er.patient_dw_id
                   AND rd_0.payor_dw_id = er.payor_dw_id
                   AND rd_0.iplan_insurance_order_num = er.iplan_insurance_order_num
                   AND rd_0.eor_log_date = er.eor_log_date
                   AND upper(rd_0.log_id) = upper(er.log_id)
                   AND rd_0.log_sequence_num = er.log_sequence_num
                   AND er.eff_from_date <= date_sub(CAST(concat(format_date('%Y-%m', current_date('US/Central')), '-01') as DATE), interval 1 DAY)
              )
               AND upper(rd_0.data_source_code) = '0125'
               AND upper(rd_0.source_system_code) IN(
                'N'
              )
              GROUP BY upper(fd_0.pas_current_name), 2, upper(rd_0.coid), upper(rd_0.log_id), 5, 6, 7, 8, 9
          ) AS cw_ss
    ) AS rd
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_dimension_eom AS fd ON upper(fd.coid) = upper(rd.coid)
  GROUP BY upper(rd.pas_current_name), upper(rd.coid), upper(fd.unit_num), upper(rd.type_code)
;
