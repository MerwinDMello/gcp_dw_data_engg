-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_pbs_j_pf_fact_patient_level_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- Locking table Edwpbs.fact_rcom_ar_patient_level for access

SELECT concat(CAST(coalesce(sum(arp.payor_payment_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.ar_patient_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.ar_insurance_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.gross_charge_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.payor_contractual_amt), NUMERIC '0') AS STRING)) AS source_string
FROM `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_ar_patient_level AS arp
WHERE arp.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                         WHEN '' THEN 0.0
                         ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS FLOAT64)
                     END
  AND arp.patient_sid <> 0
  AND arp.source_sid = 10 