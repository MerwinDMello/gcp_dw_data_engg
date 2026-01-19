-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_smry_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_smry_up_front_collection AS SELECT
    ufc.unit_num_sid,
    ufc.patient_type_sid,
    ufc.patient_financial_class_sid,
    ufc.payor_sid,
    ufc.account_status_sid,
    ufc.time_id,
    ufc.up_front_msr_sid,
    ufc.company_code,
    ufc.coid,
    sum(ufc.transaction_amt) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_up_front_collection AS ufc
  WHERE ufc.up_front_msr_sid = 800
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION DISTINCT
SELECT
    ufc.unit_num_sid,
    ufc.patient_type_sid,
    ufc.patient_financial_class_sid,
    ufc.payor_sid,
    ufc.account_status_sid,
    ufc.time_id,
    700 AS up_front_msr_sid,
    ufc.company_code,
    ufc.coid,
    CAST(count(DISTINCT ufc.pat_acct_num) as BIGNUMERIC) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_up_front_collection AS ufc
  WHERE ufc.up_front_msr_sid = 800
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION DISTINCT
SELECT
    c.unit_num_sid AS unit_num_sid,
    40 AS patient_type_sid,
    23 AS patient_financial_class_sid,
    1 AS payor_sid,
    10 AS account_status_sid,
    c.date_sid AS time_id,
    1600 AS up_front_msr_sid,
    c.company_code AS company_code,
    c.coid AS coid,
    sum(c.ip_revenue_routine_amt_fs50100 + c.ip_rev_ancillary_amt_fs50200 + c.op_ancillary_rev_amt_fs50400 + c.other_operating_income_fs50900 - (c.mcare_cy_cont_ip_amt_fs60100 + c.mcare_cy_cont_op_amt_fs60125 + c.prior_yr_cont_ip_amt_fs60150 + c.prior_yr_cont_op_amt_fs60175 + c.mcaid_cy_cont_ip_amt_fs60200 + c.mcaid_cy_cont_op_amt_fs60225 + c.champ_cy_cont_ip_amt_fs60300 + c.champ_cy_cont_op_amt_fs60325 + c.bc_hmo_ppo_disc_ip_amt_fs60400 + c.bc_hmo_ppo_disc_op_amt_fs60425 + c.mcare_mgd_care_ip_amt_fs60450 + c.mcare_mgd_care_op_amt_fs60460 + c.mcaid_mgd_care_ip_amt_fs60475 + c.mcaid_mgd_care_op_amt_fs60480 + c.charity_ip_amt_fs60500 + c.charity_op_amt_fs60525 + c.other_deduction_ip_amt_fs60600 + c.other_deduction_op_amt_fs60625 + c.bad_debt_amt_fs65500 + c.health_exchange_ip_amt_fs60490 + c.health_exchange_op_amt_fs60495)) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_hospital_level_host AS c
  WHERE c.scenario_sid = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION DISTINCT
SELECT
    CAST(/* expression of unknown or erroneous type */ sc.unit_num as INT64) AS unit_num_sid,
    coalesce(epd.patient_type_sid, 40) AS patient_type_sid,
    coalesce(efd.patient_financial_class_sid, 23) AS patient_financial_class_sid,
    1 AS payor_sid,
    10 AS account_status_sid,
    CAST(/* expression of unknown or erroneous type */ sc.rptg_period as INT64) AS time_id,
    1200 AS up_front_msr_sid,
    sc.company_code,
    sc.coid,
    sum(sc.cash_amt) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections AS sc
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_type_dim AS epd ON CASE
      WHEN upper(sc.derived_patient_type_code) = 'IE' THEN 'I'
      ELSE sc.derived_patient_type_code
    END = substr(epd.patient_type_member, 4, 3)
     AND substr(epd.patient_type_gen02, 1, 2) <> 'MC'
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_patient_fin_class_dim AS efd ON upper(CAST(/* expression of unknown or erroneous type */ sc.financial_class_code as STRING)) = upper(substr(efd.patient_financial_class_alias, 1, 2))
  WHERE sc.iplan_id = 0
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION DISTINCT
SELECT
    c.unit_num_sid AS unit_num_sid,
    40 AS patient_type_sid,
    23 AS patient_financial_class_sid,
    1 AS payor_sid,
    10 AS account_status_sid,
    c.date_sid AS time_id,
    1800 AS up_front_msr_sid,
    c.company_code AS company_code,
    c.coid AS coid,
    sum(c.bad_debt_amt_fs65500) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_hospital_level_host AS c
  WHERE c.scenario_sid = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION DISTINCT
SELECT
    ufc.unit_num_sid,
    ufc.patient_type_sid,
    ufc.patient_financial_class_sid,
    ufc.payor_sid,
    ufc.account_status_sid,
    ufc.time_id,
    ufc.up_front_msr_sid,
    ufc.company_code,
    ufc.coid,
    CAST(count(DISTINCT ufc.pat_acct_num) as BIGNUMERIC) AS transaction_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_up_front_collection AS ufc
  WHERE ufc.up_front_msr_sid = 1700
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
;
