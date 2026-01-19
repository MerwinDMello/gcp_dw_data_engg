-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_hospital_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT cah.hospital_sk AS hospital_sk,
             65 AS hospital_detail_measure_id,
             cahs.hospnumber AS hospital_detail_measure_value_text,
             CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.hospnumber IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            66 AS hospital_detail_measure_id,
                            cahs.contactfname AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactfname IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            67 AS hospital_detail_measure_id,
                            cahs.contactminit AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactminit IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            68 AS hospital_detail_measure_id,
                            cahs.contactlname AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.contactlname IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            69 AS hospital_detail_measure_id,
                            cahs.email AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.email IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            70 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.hosppfi, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            71 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.eclscenterid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            72 AS hospital_detail_measure_id,
                            CAST(NULL AS STRING) AS hospital_detail_measure_value_text,
                            cahs.participantid AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.participantid IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            73 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.encryptionkey, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            74 AS hospital_detail_measure_id,
                            cahs.auxiliary0 AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.auxiliary0 IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            75 AS hospital_detail_measure_id,
                            cahs.hospguid AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.hospguid IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            76 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.pc4siteid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            77 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.pc4encryptionkey, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            78 AS hospital_detail_measure_id,
                            hospital_detail_measure_value_text AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      CROSS JOIN UNNEST(ARRAY[ substr(cahs.cparticipantid, 1, 50) ]) AS hospital_detail_measure_value_text
      WHERE hospital_detail_measure_value_text IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            79 AS hospital_detail_measure_id,
                            cahs.acchospnpi AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.acchospnpi IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            80 AS hospital_detail_measure_id,
                            cahs.pc4hospnpi AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.pc4hospnpi IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            81 AS hospital_detail_measure_id,
                            cahs.phonenumber AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.phonenumber IS NOT NULL
      UNION DISTINCT SELECT cah.hospital_sk,
                            82 AS hospital_detail_measure_id,
                            cahs.faxnumber AS hospital_detail_measure_value_text,
                            CAST(NULL AS INT64) AS hospital_detail_measure_value_num
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS cas
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(cas.server_name) = upper(cahs.full_server_nm)
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_hospital AS cah
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cas.server_sk = cah.server_sk
      AND cahs.hospitalid = cah.source_hospital_id
      WHERE cahs.faxnumber IS NOT NULL ) AS a
   WHERE upper(trim(a.hospital_detail_measure_value_text)) <> ''
     OR a.hospital_detail_measure_value_num IS NOT NULL ) AS b