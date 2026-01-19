-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_unbilled_inv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_unbilled_inv AS SELECT
    ui.company_code,
    ui.coid,
    ui.patient_type_code_pos1,
    max(ui.effective_date) AS effective_date,
    dpyr.payor_sid,
    sum(CASE
      WHEN ui.account_process_ind = 'I' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS in_hse_acct_bal_amt,
    sum(CASE
      WHEN ui.account_process_ind = 'D' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS lt_summ_days_acct_bal_amt,
    sum(CASE
      WHEN ui.account_process_ind = 'S'
       AND ui.unbilled_responsibility_ind = 'M' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS gt_summ_days_bus_acct_bal_amt,
    sum(CASE
      WHEN ui.account_process_ind = 'S'
       AND ui.unbilled_responsibility_ind = 'B' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS gt_summ_days_med_acct_bal_amt,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code IN(
        '1'
      )
       AND ui.ssi_acct_type_desc <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS error_acct_bal_amt,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '1'
       AND ui.ssi_acct_type_desc = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS error_acct_bal_amt_msc,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '2'
       AND ui.ssi_acct_type_desc = 'PAS' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_pas,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '2'
       AND ui.ssi_acct_type_desc = 'Facility' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_fac,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '2'
       AND ui.ssi_acct_type_desc = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_msc,
    sum(CASE
      WHEN ui.final_bill_hold_ind = 'Y' THEN ui.total_account_balance_amt
      ELSE CAST(0 as NUMERIC)
    END) AS hold_acct_bal_amt_final_bill,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '3'
       AND ui.ssi_acct_type_desc <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS valid_acct_bal_amt,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '3'
       AND ui.ssi_acct_type_desc = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS valid_acct_bal_amt_msc,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code IN(
        '8'
      )
       AND ui.ssi_acct_type_desc <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS wait_acct_bal_amt,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = '8'
       AND ui.ssi_acct_type_desc = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS wait_acct_bal_amt_msc,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code IN(
        'A'
      )
       AND ui.ssi_acct_type_desc <> 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS add_acct_bal_amt,
    sum(CASE
      WHEN ui.ssi_unbilled_status_code = 'A'
       AND ui.ssi_acct_type_desc = 'MSC' THEN ui.gross_charge_amt
      ELSE CAST(0 as NUMERIC)
    END) AS add_acct_bal_amt_msc
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.unbilled_inventory AS ui
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient AS fp ON ui.coid = fp.coid
     AND ui.pat_acct_num = fp.pat_acct_num
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.rcom_payor_dimension_eom AS pyr ON fp.payor_dw_id_ins1 = pyr.payor_dw_id
     AND (fp.discharge_date IS NOT NULL
     AND fp.discharge_date BETWEEN pyr.eff_from_date AND pyr.eff_to_date
     OR fp.discharge_date IS NULL
     AND fp.admission_date BETWEEN pyr.eff_from_date AND pyr.eff_to_date)
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_payor AS dpyr ON pyr.payor_id = dpyr.payor_id
  GROUP BY 1, 2, 3, upper(ui.effective_date), 5
;
