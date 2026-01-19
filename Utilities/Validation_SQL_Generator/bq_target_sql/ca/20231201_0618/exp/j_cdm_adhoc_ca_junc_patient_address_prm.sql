-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_junc_patient_address_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          cap.patient_sk,
          'R' AS address_type_code,
          caa.address_sk
        FROM
          `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg AS cads
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cads.full_server_nm) = upper(cas.server_name)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_patient AS cap ON cads.patid = cap.source_patient_id
           AND cas.server_sk = cap.server_sk
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_address AS caa ON upper(coalesce(cads.pataddr, 'NULL')) = upper(coalesce(caa.address_line_1_text, 'NULL'))
           AND upper(coalesce(cads.pataddr2, 'NULL')) = upper(coalesce(caa.address_line_2_text, 'NULL'))
           AND upper(coalesce(cads.patcity, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
           AND upper(coalesce(cads.patstate, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
           AND upper(coalesce(cads.patzip, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
           AND upper(coalesce(cads.county, 'NULL')) = upper(coalesce(caa.county_name, 'NULL'))
           AND coalesce(format('%11d', cads.country), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL')
           AND caa.address_line_3_text IS NULL
      UNION DISTINCT
      SELECT DISTINCT
          cap.patient_sk,
          'B' AS address_type_code,
          caa.address_sk
        FROM
          `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg AS cads
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS cas ON upper(cads.full_server_nm) = upper(cas.server_name)
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_patient AS cap ON cads.patid = cap.source_patient_id
           AND cas.server_sk = cap.server_sk
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_address AS caa ON upper(coalesce(cads.birthcit, 'NULL')) = upper(coalesce(caa.city_name, 'NULL'))
           AND upper(coalesce(cads.birthsta, 'NULL')) = upper(coalesce(caa.state_name, 'NULL'))
           AND upper(coalesce(cads.birthzip, 'NULL')) = upper(coalesce(caa.zip_code, 'NULL'))
           AND coalesce(format('%11d', cads.birthcou), 'NULL') = coalesce(format('%11d', caa.country_id), 'NULL')
           AND caa.address_line_1_text IS NULL
           AND caa.address_line_2_text IS NULL
           AND caa.address_line_3_text IS NULL
           AND caa.county_name IS NULL
    ) AS b
;
