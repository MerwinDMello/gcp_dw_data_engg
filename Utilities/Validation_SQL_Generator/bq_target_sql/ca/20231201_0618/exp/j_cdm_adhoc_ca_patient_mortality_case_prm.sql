-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_mortality_case_prm.sql
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
            SELECT DISTINCT
                NULL AS patient_mortality_case_sk,
                a_0.patient_case_sk AS patient_case_sk,
                stg.mt30stat AS mortality_30_day_id,
                stg.mt30statmeth AS mortality_30_day_method_id,
                stg.mortcase AS mortality_case_id,
                csrv.server_sk AS server_sk,
                stg.mortcasesid AS source_mortality_case_id,
                stg.mtopd AS mortality_operative_death_id,
                stg.createdate AS source_create_date_time,
                stg.updatedate AS source_last_update_date_time,
                stg.updateby AS updated_by_3_4_id,
                'C' AS source_system_code,
                timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_mortcases_stg AS stg
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
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_patient_mortality_case AS ch ON ch.server_sk = csrv.server_sk
                 AND ch.source_mortality_case_id = stg.mortcasesid
              WHERE ch.server_sk IS NULL
               AND ch.source_mortality_case_id IS NULL
          ) AS a
    ) AS b
;
