-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_case_diagnosis_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          a.*
        FROM
          (
            SELECT
                NULL AS case_diagnosis_sk,
                a_0.patient_case_sk AS patient_case_sk,
                csrv.server_sk AS server_sk,
                d.diagnosis_list_sk AS diagnosis_list_sk,
                stg.diagnosisid AS source_case_diagnosis_id,
                CASE
                  WHEN stg.icd_9code IS NOT NULL THEN stg.icd_9code
                  WHEN stg.cptcode IS NOT NULL THEN stg.cptcode
                  WHEN stg.icd_10code IS NOT NULL THEN stg.icd_10code
                END AS source_diagnosis_code_text,
                CASE
                   upper(ch.source_diagnosis_code_text)
                  WHEN upper(stg.icd_9code) THEN '9'
                  WHEN upper(stg.cptcode) THEN '5'
                  WHEN upper(stg.icd_10code) THEN '10'
                END AS diagnosis_type_code,
                stg.diagshrtlst AS diagnosis_short_text,
                stg.primdiag AS primary_diagnosis_id,
                stg.sort AS source_sort_num,
                stg.createdate AS source_create_date_time,
                stg.updatedate AS source_last_update_date_time,
                stg.updateby AS updated_by_3_4_id,
                'C' AS source_system_code,
                timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosis_stg AS stg
                LEFT OUTER JOIN (
                  SELECT
                      c.source_patient_case_num,
                      s.server_name,
                      c.patient_case_sk
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm.ca_patient_case AS c
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON c.server_sk = s.server_sk
                ) AS a_0 ON stg.casenumber = a_0.source_patient_case_num
                 AND upper(stg.full_server_nm) = upper(a_0.server_name)
                INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(stg.full_server_nm) = upper(csrv.server_name)
                LEFT OUTER JOIN (
                  SELECT
                      c.source_diagnosis_list_id,
                      s.server_name,
                      c.diagnosis_list_sk
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm.ca_diagnosis_list AS c
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON c.server_sk = s.server_sk
                ) AS d ON stg.diagid = d.source_diagnosis_list_id
                 AND upper(stg.full_server_nm) = upper(d.server_name)
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_case_diagnosis AS ch ON ch.server_sk = csrv.server_sk
                 AND ch.source_case_diagnosis_id = stg.diagnosisid
              WHERE ch.server_sk IS NULL
               AND ch.source_case_diagnosis_id IS NULL
          ) AS a
    ) AS b
;
