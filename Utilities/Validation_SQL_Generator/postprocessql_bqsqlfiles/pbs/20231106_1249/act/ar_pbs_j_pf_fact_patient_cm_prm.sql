-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_pbs_j_pf_fact_patient_cm_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT trim(format('%20d', coalesce(count(*), 0))) AS source_string
FROM `hca-hin-dev-cur-parallon`.auth_base_views.fact_rcom_ar_patient_lvl_cm 