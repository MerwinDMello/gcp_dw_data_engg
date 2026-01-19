-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_address_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT wrk1.*
   FROM
     (SELECT DISTINCT CASE
                          WHEN upper(trim(wrk1_0.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.address_line_1_text)
                      END AS address_line_1_text,
                      CASE
                          WHEN upper(trim(wrk1_0.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.address_line_2_text)
                      END AS address_line_2_text,
                      CASE
                          WHEN trim(format('%11d', wrk1_0.address_line_3_text)) = '' THEN CAST(NULL AS STRING)
                          ELSE substr(trim(format('%11d', wrk1_0.address_line_3_text)), 1, 100)
                      END AS address_line_3_text,
                      CASE
                          WHEN upper(trim(wrk1_0.city_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.city_name)
                      END AS city_name,
                      CASE
                          WHEN upper(trim(wrk1_0.state_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.state_name)
                      END AS state_name,
                      CASE
                          WHEN upper(trim(wrk1_0.zip_code)) = '' THEN CAST(NULL AS STRING)
                          ELSE trim(wrk1_0.zip_code)
                      END AS zip_code,
                      CASE
                          WHEN upper(trim(wrk1_0.county_name)) = '' THEN CAST(NULL AS STRING)
                          ELSE substr(trim(wrk1_0.county_name), 1, 50)
                      END AS county_name,
                      CASE
                          WHEN trim(format('%11d', wrk1_0.country_id)) = '' THEN CAST(NULL AS INT64)
                          ELSE CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(trim(format('%11d', wrk1_0.country_id))) AS INT64)
                      END AS country_id
      FROM
        (SELECT DISTINCT cardioaccess_demographics_stg.pataddr AS address_line_1_text,
                         cardioaccess_demographics_stg.pataddr2 AS address_line_2_text,
                         CAST(NULL AS INT64) AS address_line_3_text,
                         substr(cardioaccess_demographics_stg.patcity, 1, 50) AS city_name,
                         cardioaccess_demographics_stg.patstate AS state_name,
                         cardioaccess_demographics_stg.patzip AS zip_code,
                         substr(cardioaccess_demographics_stg.county, 1, 50) AS county_name,
                         cardioaccess_demographics_stg.country AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg
         UNION ALL SELECT DISTINCT CAST(NULL AS STRING) AS address_line_1_text,
                                   CAST(NULL AS STRING) AS address_line_2_text,
                                   CAST(NULL AS INT64) AS address_line_3_text,
                                   substr(cardioaccess_demographics_stg.birthcit, 1, 50) AS city_name,
                                   substr(cardioaccess_demographics_stg.birthsta, 1, 2) AS state_name,
                                   cardioaccess_demographics_stg.birthzip AS zip_code,
                                   CAST(NULL AS STRING) AS county_name,
                                   cardioaccess_demographics_stg.birthcou AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_demographics_stg
         UNION ALL SELECT DISTINCT substr(cacs.address, 1, 150) AS address_line_1_text,
                                   substr(cacs.address2, 1, 60) AS address_line_2_text,
                                   CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(cacs.address3) AS INT64) AS address_line_3_text,
                                   cacs.city AS city_name,
                                   substr(cacs.stateorprovince, 1, 2) AS state_name,
                                   substr(cacs.postalcode, 1, 15) AS zip_code,
                                   cacs.county AS county_name,

           (SELECT rcgl.lookup_id
            FROM {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_global_lookup AS rcgl
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
            WHERE upper(rcgl.sts_code_text) = upper(cacs.country)
              AND upper(rcgl.short_name) = 'ISOCOUNTRY' ) AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_contacts_stg AS cacs
         UNION ALL SELECT DISTINCT substr(cahs.hospaddress, 1, 150) AS address_line_1_text,
                                   substr(cahs.hospaddress2, 1, 60) AS address_line_2_text,
                                   CAST(NULL AS INT64) AS address_line_3_text,
                                   cahs.hospcity AS city_name,
                                   cahs.hospstate AS state_name,
                                   substr(cahs.hospzip, 1, 15) AS zip_code,
                                   CAST(NULL AS STRING) AS county_name,

           (SELECT rcgl.lookup_id
            FROM {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_global_lookup AS rcgl
            FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
            WHERE upper(rcgl.sts_code_text) = upper(cahs.hospcountry)
              AND upper(rcgl.short_name) = 'ISOCOUNTRY' ) AS country_id
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_hospital_stg AS cahs) AS wrk1_0
      WHERE CASE
                WHEN upper(trim(wrk1_0.address_line_1_text)) = '' THEN CAST(NULL AS STRING)
                ELSE trim(wrk1_0.address_line_1_text)
            END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.address_line_2_text)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.address_line_2_text)
           END IS NOT NULL
        OR CASE
               WHEN trim(format('%11d', wrk1_0.address_line_3_text)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(format('%11d', wrk1_0.address_line_3_text))
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.city_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.city_name)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.state_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.state_name)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.zip_code)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.zip_code)
           END IS NOT NULL
        OR CASE
               WHEN upper(trim(wrk1_0.county_name)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(wrk1_0.county_name)
           END IS NOT NULL
        OR CASE
               WHEN trim(format('%11d', wrk1_0.country_id)) = '' THEN CAST(NULL AS STRING)
               ELSE trim(format('%11d', wrk1_0.country_id))
           END IS NOT NULL ) AS wrk1
   WHERE NOT EXISTS
       (SELECT 1
        FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_address AS caa
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
        WHERE upper(coalesce(wrk1.address_line_1_text, 'NULL')) = upper(coalesce(caa.address_line_1_text, 'NULL'))
          AND upper(coalesce(wrk1.address_line_2_text, 'NULL')) = upper(coalesce(caa.address_line_2_text, 'NULL'))
          AND upper(coalesce(wrk1.address_line_3_text, 'NULL')) = upper(coalesce(caa.address_line_3_text, 'NULL'))
          AND upper(coalesce(wrk1.city_name, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
          AND upper(coalesce(wrk1.state_name, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
          AND upper(coalesce(wrk1.zip_code, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
          AND upper(coalesce(wrk1.county_name, 'NULL')) = upper(coalesce(caa.county_name, 'NULL'))
          AND coalesce(format('%11d', wrk1.country_id), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL') ) ) AS a