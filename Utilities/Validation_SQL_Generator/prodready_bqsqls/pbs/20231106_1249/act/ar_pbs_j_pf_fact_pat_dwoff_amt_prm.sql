-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_pbs_j_pf_fact_pat_dwoff_amt_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- Locking table edwpbs.Fact_RCOM_AR_patient_Level for access

SELECT concat(coalesce(CAST(sum(fact_rcom_ar_patient_level.payor_denial_amt) AS STRING), '0'), coalesce(format('%11d', sum(fact_rcom_ar_patient_level.payor_denial_cnt)), '0')) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_ar_patient_level
WHERE fact_rcom_ar_patient_level.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                WHEN '' THEN 0
                                                ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                                            END