-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_j_pf_fact_pat_dchrg_cnt_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- Locking table edwpf.Fact_RCOM_AR_Patient_Level for access
SELECT
    concat(trim(format('%11d', coalesce(sum(fact_rcom_ar_patient_level.billed_patient_cnt), 0))), trim(CAST(sum(CAST(fact_rcom_ar_patient_level.discharge_to_billing_day_cnt as NUMERIC)) as STRING))) AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpf.fact_rcom_ar_patient_level
  WHERE fact_rcom_ar_patient_level.date_sid = CASE
     format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
    WHEN '' THEN 0
    ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) as INT64)
  END
;
