-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_unbilled_inv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_unbilled_inv AS SELECT
    max(ui.company_code) AS company_code,
    max(ui.coid) AS coid,
    max(ui.patient_type_code_pos1) AS patient_type_code_pos1,
    max(ui.effective_date) AS effective_date,
    dpyr.payor_sid,
    sum(CASE
      WHEN upper(ui.account_process_ind) = 'I' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS in_hse_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.account_process_ind) = 'D' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS lt_summ_days_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.account_process_ind) = 'S'
       AND upper(ui.unbilled_responsibility_ind) = 'M' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS gt_summ_days_bus_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.account_process_ind) = 'S'
       AND upper(ui.unbilled_responsibility_ind) = 'B' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS gt_summ_days_med_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) IN(
        '1'
      )
       AND upper(ui.ssi_acct_type_desc) <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS error_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '1'
       AND upper(ui.ssi_acct_type_desc) = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS error_acct_bal_amt_msc,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '2'
       AND upper(ui.ssi_acct_type_desc) = 'PAS' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_pas,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '2'
       AND upper(ui.ssi_acct_type_desc) = 'FACILITY' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_fac,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '2'
       AND upper(ui.ssi_acct_type_desc) = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_msc,
    sum(CASE
      WHEN upper(ui.final_bill_hold_ind) = 'Y' THEN ui.total_account_balance_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_final_bill,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '3'
       AND upper(ui.ssi_acct_type_desc) <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS valid_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '3'
       AND upper(ui.ssi_acct_type_desc) = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS valid_acct_bal_amt_msc,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) IN(
        '8'
      )
       AND upper(ui.ssi_acct_type_desc) <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS wait_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = '8'
       AND upper(ui.ssi_acct_type_desc) = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS wait_acct_bal_amt_msc,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) IN(
        'A'
      )
       AND upper(ui.ssi_acct_type_desc) <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS add_acct_bal_amt,
    sum(CASE
      WHEN upper(ui.ssi_unbilled_status_code) = 'A'
       AND upper(ui.ssi_acct_type_desc) = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS add_acct_bal_amt_msc
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.unbilled_inventory AS ui
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp ON upper(ui.coid) = upper(fp.coid)
     AND ui.pat_acct_num = fp.pat_acct_num
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.rcom_payor_dimension_eom AS pyr ON fp.payor_dw_id_ins1 = pyr.payor_dw_id
     AND (fp.discharge_date IS NOT NULL
     AND fp.discharge_date BETWEEN pyr.eff_from_date AND pyr.eff_to_date
     OR fp.discharge_date IS NULL
     AND fp.admission_date BETWEEN pyr.eff_from_date AND pyr.eff_to_date)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_payor AS dpyr ON pyr.payor_id = dpyr.payor_id
  GROUP BY upper(ui.company_code), upper(ui.coid), upper(ui.patient_type_code_pos1), upper(ui.effective_date), 5
;
