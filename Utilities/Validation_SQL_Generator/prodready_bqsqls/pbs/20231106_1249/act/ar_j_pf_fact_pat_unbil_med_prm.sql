-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_j_pf_fact_pat_unbil_med_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT trim(CAST(coalesce(sum(fact_rcom_ar_patient_level.unbilled_gross_med_rec_amt), NUMERIC '0') AS STRING)) AS source_string
FROM {{ params.param_auth_base_views_dataset_name }}.fact_rcom_ar_patient_level
WHERE fact_rcom_ar_patient_level.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                WHEN '' THEN 0
                                                ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                                            END