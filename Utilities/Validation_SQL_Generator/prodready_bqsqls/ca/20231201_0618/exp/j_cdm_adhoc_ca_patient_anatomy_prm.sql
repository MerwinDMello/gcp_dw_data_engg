-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_anatomy_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT NULL AS patient_anatomy_sk,
             a_0.patient_sk,
             csrv.server_sk,
             stg.patanatomyid AS source_patient_anatomy_id,
             stg.funddiag AS fundamental_diagnosis_id,
             stg.premature AS premature_birth_id,
             stg.gestageweeks AS gestation_age_week_num,
             stg.birthwtkg AS birth_weight_kg_amt,
             stg.antdiag AS antenatal_diagnosis_id,
             stg.apgar1min AS apgar_score_1_min_num,
             stg.apgar5min AS apgar_score_5_min_num,
             stg.funddxtext AS fundamental_diagnosis_text,
             stg.birthlen AS birth_length_cm_num,
             stg.birthhcircum AS birth_head_circum_cm_num,
             stg.createdate AS source_create_date_time,
             stg.lastupdate AS source_last_update_date_time,
             stg.updatedby AS updated_by_3_4_id,
             stg.birthivf AS birth_ivf_id,
             stg.bornloc AS born_in_out_id,
             stg.multgest AS multiple_gestation_id,
             stg.delivmode AS delivery_mode_id,
             stg.gravidity AS mother_gravidity_num,
             stg.parity AS mother_parity_num,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_patanatomy_stg AS stg
      LEFT OUTER JOIN
        (SELECT c.source_patient_id,
                s.server_name,
                c.patient_sk
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.patid = a_0.source_patient_id
      AND upper(stg.full_server_nm) = upper(a_0.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_anatomy AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_patient_anatomy_id = stg.patanatomyid
      WHERE ch.server_sk IS NULL
        AND ch.source_patient_anatomy_id IS NULL ) AS a) AS b