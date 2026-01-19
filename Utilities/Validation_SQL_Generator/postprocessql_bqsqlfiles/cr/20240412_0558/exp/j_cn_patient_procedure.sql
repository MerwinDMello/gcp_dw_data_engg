-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_procedure.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.cn_patient_procedure_sid,
          stg.core_record_type_id,
          refpr.procedure_type_id,
          stg.med_spcl_physician_id,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.procedure_date,
          stg.palliative_ind,
          stg.hashbite_ssk,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_procedure_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_procedure_type AS refpr ON upper(rtrim(stg.proceduretype)) = upper(rtrim(refpr.procedure_type_desc))
   AND upper(rtrim(coalesce(stg.otherproceduretype, stg.othersurgerytype, stg.lineplacementtype, 'XX'))) = upper(rtrim(coalesce(refpr.procedure_sub_type_desc, 'XX')))
   WHERE upper(trim(stg.hashbite_ssk)) NOT IN
       (SELECT upper(trim(cn_patient_procedure.hashbite_ssk))
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_procedure) ) AS a