-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_ref_rad_onc_diagnosis_code.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT row_number() OVER (
                             ORDER BY dp.dimsiteid,
                                      dp.dimdiagnosiscodeid DESC) AS diagnosis_code_sk,
                            rr.site_sk AS site_sk,
                            dp.dimdiagnosiscodeid AS source_diagnosis_code_id,
                            td_sysfnlib.decode(dp.diagnosiscode, '', NULL, dp.diagnosiscode) AS diagnosis_code,
                            sc.diagnosissites AS diagnosis_site_text,
                            dp.diagnosiscodeclsschemeid AS diagnosis_code_class_schema_id,
                            td_sysfnlib.decode(dp.diagnosisclinicaldescriptionen, '', NULL, dp.diagnosisclinicaldescriptionen) AS diagnosis_clinical_desc,
                            td_sysfnlib.decode(dp.diagnosisfulltitleenu, '', NULL, dp.diagnosisfulltitleenu) AS diagnosis_long_desc,
                            td_sysfnlib.decode(dp.diagnosistableenu, '', NULL, dp.diagnosistableenu) AS diagnosis_type_code,
                            dp.logid AS log_id,
                            dp.runid AS run_id,
                            'R' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT dimdiagnosiscode_stg.dimdiagnosiscodeid,
             dimdiagnosiscode_stg.dimsiteid,
             dimdiagnosiscode_stg.diagnosiscodeclsschemeid,
             trim(dimdiagnosiscode_stg.diagnosiscode) AS diagnosiscode,
             trim(dimdiagnosiscode_stg.diagnosisclinicaldescriptionen) AS diagnosisclinicaldescriptionen,
             trim(dimdiagnosiscode_stg.diagnosisfulltitleenu) AS diagnosisfulltitleenu,
             trim(dimdiagnosiscode_stg.diagnosistableenu) AS diagnosistableenu,
             dimdiagnosiscode_stg.logid,
             dimdiagnosiscode_stg.runid
      FROM {{ params.param_cr_stage_dataset_name }}.dimdiagnosiscode_stg) AS dp
   INNER JOIN
     (SELECT ref_rad_onc_site.source_site_id,
             ref_rad_onc_site.site_sk
      FROM {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) AS rr ON rr.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN
     (SELECT DISTINCT trim(cr_sc_diagnosissites_stg.diagnosiscode) AS diagnosiscode,
                      cr_sc_diagnosissites_stg.diagnosissites
      FROM {{ params.param_cr_stage_dataset_name }}.cr_sc_diagnosissites_stg) AS sc ON rtrim(sc.diagnosiscode) = rtrim(dp.diagnosiscode)) AS stg