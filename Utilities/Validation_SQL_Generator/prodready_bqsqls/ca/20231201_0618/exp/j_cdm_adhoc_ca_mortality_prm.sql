-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_mortality_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT a.*
   FROM
     (SELECT NULL AS mortality_sk,
             a_0.patient_sk AS patient_sk,
             ca_server.server_sk AS server_sk,
             trim(format('%11d', stg.mtid)) AS source_mortality_id,
             CAST(trim(concat(substr(CAST(DATE(stg.mtdate) AS STRING), 1, 10), ' ', CAST(time(stg.mttime) AS STRING))) AS DATETIME) AS mortality_date_time,
             trim(format('%11d', stg.mtlocation)) AS mortality_location_id,
             trim(format('%11d', stg.mortalty)) AS mortality_hosp_id,
             trim(format('%11d', stg.primarycause)) AS primarycause,
             trim(format('%11d', stg.cardiac)) AS cardiac,
             trim(format('%11d', stg.renal)) AS renal,
             trim(format('%11d', stg.infection)) AS infection,
             trim(format('%11d', stg.valvular)) AS valvular,
             trim(format('%11d', stg.neurologic)) AS neurologic,
             trim(format('%11d', stg.vascular)) AS vascular,
             trim(format('%11d', stg.pulmonary)) AS pulmonary,
             trim(format('%11d', stg.other)) AS other,
             trim(format('%11d', stg.gi)) AS gi,
             trim(format('%11d', stg.prematurity)) AS prematurity,
             trim(format('%11d', stg.deathlab)) AS deathlab,
             trim(format('%11d', stg.autopsy)) AS autopsy,
             trim(stg.autopsydx) AS autopsydx,
             trim(stg.suspectedcausedeath) AS suspectedcausedeath,
             trim(format('%11d', stg.mtage)) AS mtage,
             trim(format('%11d', stg.deathcause)) AS deathcause,
             CAST(trim(CAST(stg.createdate AS STRING)) AS DATETIME) AS source_create_date_time,
             CAST(trim(CAST(stg.lastupdate AS STRING)) AS DATETIME) AS source_last_update_date_time,
             trim(stg.updateby) AS updated_by_3_4_id,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_mortality_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ca_server.server_name)
      LEFT OUTER JOIN
        (SELECT c.patient_sk,
                c.source_patient_id,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON c.server_sk = s.server_sk) AS a_0 ON stg.patid = a_0.source_patient_id
      AND upper(stg.full_server_nm) = upper(a_0.server_name)) AS a) AS b