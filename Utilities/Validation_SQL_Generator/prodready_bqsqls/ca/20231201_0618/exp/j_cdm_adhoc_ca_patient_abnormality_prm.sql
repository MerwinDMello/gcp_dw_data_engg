-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_abnormality_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT b.*
   FROM
     (SELECT CAST(NULL AS INT64) AS patient_abnormality_sk,
             a.patient_sk AS patient_sk,
             csrv.server_sk AS server_sk,
             1 AS abnormality_type_id,
             css.synuniqueid AS source_patient_abnormality_id,
             css.syndrome AS abnormality_id,
             css.syndromeothsp AS abnormality_other_text,
             css.demodatavrsn AS demographic_data_version_num_code,
             CAST(DATE(css.createdate) AS DATETIME) AS source_create_date_time,
             CAST(DATE(css.lastupdate) AS DATETIME) AS source_last_update_date_time,
             css.updateby AS updated_by_3_4_id,
             css.sort AS source_sort_num,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_syndrome_stg AS css
      LEFT OUTER JOIN
        (SELECT c.source_patient_id,
                s.server_name,
                c.patient_sk
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a ON css.patid = a.source_patient_id
      AND upper(css.full_server_nm) = upper(a.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(css.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_abnormality AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_patient_abnormality_id = css.synuniqueid
      WHERE ch.server_sk IS NULL
        AND ch.source_patient_abnormality_id IS NULL
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_abnormality_sk,
                            a.patient_sk AS patient_sk,
                            csrv.server_sk AS server_sk,
                            2 AS abnormality_type_id,
                            ccab.chromabuniqueid AS source_patient_abnormality_id,
                            ccab.chromab AS abnormality_id,
                            substr(ccab.chromabothsp, 1, 255) AS abnormality_other_text,
                            substr(ccab.demodatavrsn, 1, 255) AS demographic_data_version_num_code,
                            CAST(DATE(ccab.createdate) AS DATETIME) AS source_create_date_time,
                            CAST(DATE(ccab.lastupdate) AS DATETIME) AS source_last_update_date_time,
                            substr(ccab.updateby, 1, 255) AS updated_by_3_4_id,
                            ccab.sort AS source_sort_num,
                            'C' AS source_system_code,
                            timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_chromabnormalities_stg AS ccab
      LEFT OUTER JOIN
        (SELECT c.source_patient_id,
                s.server_name,
                c.patient_sk
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a ON ccab.patid = a.source_patient_id
      AND upper(ccab.full_server_nm) = upper(a.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(ccab.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_abnormality AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_patient_abnormality_id = ccab.chromabuniqueid
      WHERE ch.server_sk IS NULL
        AND ch.source_patient_abnormality_id IS NULL
      UNION DISTINCT SELECT CAST(NULL AS INT64) AS patient_abnormality_sk,
                            a.patient_sk AS patient_sk,
                            csrv.server_sk AS server_sk,
                            3 AS abnormality_type_id,
                            ns.ncaauniqueid AS source_patient_abnormality_id,
                            ns.ncaa AS abnormality_id,
                            substr(ns.ncaaothsp, 1, 255) AS abnormality_other_text,
                            substr(ns.demodatavrsn, 1, 255) AS demographic_data_version_num_code,
                            CAST(DATE(ns.createdate) AS DATETIME) AS source_create_date_time,
                            CAST(DATE(ns.lastupdate) AS DATETIME) AS source_last_update_date_time,
                            substr(ns.updatedby, 1, 255) AS updated_by_3_4_id,
                            ns.sort AS source_sort_num,
                            'C' AS source_system_code,
                            timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_ncaa_stg AS ns
      LEFT OUTER JOIN
        (SELECT c.source_patient_id,
                s.server_name,
                c.patient_sk
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a ON ns.patid = a.source_patient_id
      AND upper(ns.full_server_nm) = upper(a.server_name)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(ns.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_abnormality AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.server_sk = csrv.server_sk
      AND ch.source_patient_abnormality_id = ns.ncaauniqueid
      WHERE ch.server_sk IS NULL
        AND ch.source_patient_abnormality_id IS NULL ) AS b) AS c