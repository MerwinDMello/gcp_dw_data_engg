-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_heme.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.patienthemefactid,
          stg.patientdimid,
          stg.tumortypedimid,
          stg.diagnosisresultid,
          stg.diagnosisdimid,
          max(stg.coid) AS coid,
          'H' AS company_code,
          stg.navigatordimid,
          max(stg.transportation) AS transportation,
          max(stg.drugusehistory) AS drugusehistory,
          dtl.physician_id,
          max(stg.hbsource) AS hbsource,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS dtl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(dtl.physician_name)) = upper(rtrim(stg.hematologist))
   WHERE upper(rtrim(stg.hbsource)) NOT IN
       (SELECT upper(rtrim(tgt.hashbite_ssk)) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme AS tgt
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
   GROUP BY 1,
            2,
            3,
            4,
            5,
            upper(stg.coid),
            7,
            8,
            upper(stg.transportation),
            upper(stg.drugusehistory),
            11,
            upper(stg.hbsource),
            13,
            14 QUALIFY row_number() OVER (PARTITION BY stg.patienthemefactid
                                          ORDER BY dtl.physician_id DESC) = 1) AS a