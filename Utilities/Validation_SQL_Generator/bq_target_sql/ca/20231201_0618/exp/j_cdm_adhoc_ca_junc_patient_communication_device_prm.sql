-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_junc_patient_communication_device_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          unions.communication_device_type_code,
          unions.communication_device_num,
          unions.patid,
          unions.full_server_nm
        FROM
          (
            SELECT
                cardioaccess_demographics_stg.patid,
                'H' AS communication_device_type_code,
                trim(cardioaccess_demographics_stg.patphone) AS communication_device_num,
                cardioaccess_demographics_stg.full_server_nm
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
            UNION DISTINCT
            SELECT
                cardioaccess_demographics_stg.patid,
                'F' AS communication_device_type_code,
                trim(cardioaccess_demographics_stg.patfax) AS communication_device_num,
                cardioaccess_demographics_stg.full_server_nm
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
            UNION DISTINCT
            SELECT
                cardioaccess_demographics_stg.patid,
                'W' AS communication_device_type_code,
                trim(cardioaccess_demographics_stg.patwphone) AS communication_device_num,
                cardioaccess_demographics_stg.full_server_nm
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
            UNION DISTINCT
            SELECT
                cardioaccess_demographics_stg.patid,
                'C' AS communication_device_type_code,
                trim(cardioaccess_demographics_stg.patcphone) AS communication_device_num,
                cardioaccess_demographics_stg.full_server_nm
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
            UNION DISTINCT
            SELECT
                cardioaccess_demographics_stg.patid,
                'P' AS communication_device_type_code,
                trim(cardioaccess_demographics_stg.pager) AS communication_device_num,
                cardioaccess_demographics_stg.full_server_nm
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
          ) AS unions
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_communication_device AS cad ON upper(unions.communication_device_num) = upper(cad.communication_device_num)
        WHERE unions.communication_device_num IS NOT NULL
    ) AS a
;
