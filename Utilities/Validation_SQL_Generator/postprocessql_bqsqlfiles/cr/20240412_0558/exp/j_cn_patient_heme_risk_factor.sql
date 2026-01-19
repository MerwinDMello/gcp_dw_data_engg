-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_heme_risk_factor.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.patienthemediagnosisfactid AS cn_patient_heme_diagnosis_sid,
          stg.patientdimid AS nav_patient_id,
          stg.tumortypedimid AS tumor_type_id,
          stg.diagnosisresultid AS diagnosis_result_id,
          stg.diagnosisdimid AS nav_diagnosis_id,
          stg.coid AS coid,
          'H' AS company_code,
          stg.navigatordimid AS navigator_id,
          trim(stg.riskfactor) AS risk_factor_text,
          trim(stg.otherriskfactor) AS other_risk_factor_text,
          prev.site_location_id AS previous_tumor_site_id,
          oth_prev.site_location_id AS other_previous_tumor_site_id,
          stg.hashbite_ssk,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_risk_factor_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_site_location AS oth_prev ON upper(rtrim(stg.othertumordiseasesite)) = upper(rtrim(oth_prev.site_location_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_site_location AS prev ON upper(rtrim(stg.tumordiseasesite)) = upper(rtrim(prev.site_location_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor AS tgt ON upper(rtrim(stg.hashbite_ssk)) = upper(rtrim(tgt.hashbite_ssk))
   WHERE tgt.hashbite_ssk IS NULL
     AND upper(trim(tgt.risk_factor_text)) <> upper(rtrim(tgt.risk_factor_text)) ) AS a