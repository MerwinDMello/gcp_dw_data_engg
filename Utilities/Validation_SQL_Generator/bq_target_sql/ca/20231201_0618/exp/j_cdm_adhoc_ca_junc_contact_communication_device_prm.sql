-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_junc_contact_communication_device_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          wrk0.communication_device_type_code,
          wrk0.communication_device_num,
          wrk0.contactid,
          wrk0.full_server_nm
        FROM
          (
            SELECT DISTINCT
                unions.communication_device_type_code,
                unions.communication_device_num,
                unions.contactid,
                unions.full_server_nm
              FROM
                (
                  SELECT
                      cardioaccess_contacts_stg.contactid,
                      'W' AS communication_device_type_code,
                      CASE
                        WHEN cardioaccess_contacts_stg.workextension IS NOT NULL THEN concat(trim(cardioaccess_contacts_stg.workphone), ':', trim(cardioaccess_contacts_stg.workextension))
                        ELSE trim(cardioaccess_contacts_stg.workphone)
                      END AS communication_device_num,
                      cardioaccess_contacts_stg.full_server_nm
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
                  UNION DISTINCT
                  SELECT
                      cardioaccess_contacts_stg.contactid,
                      'P' AS communication_device_type_code,
                      trim(cardioaccess_contacts_stg.pagernum) AS communication_device_num,
                      cardioaccess_contacts_stg.full_server_nm
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
                  UNION DISTINCT
                  SELECT
                      cardioaccess_contacts_stg.contactid,
                      'H' AS communication_device_type_code,
                      trim(cardioaccess_contacts_stg.homephone) AS communication_device_num,
                      cardioaccess_contacts_stg.full_server_nm
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
                  UNION DISTINCT
                  SELECT
                      cardioaccess_contacts_stg.contactid,
                      'C' AS communication_device_type_code,
                      trim(cardioaccess_contacts_stg.mobilephone) AS communication_device_num,
                      cardioaccess_contacts_stg.full_server_nm
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
                  UNION DISTINCT
                  SELECT
                      cardioaccess_contacts_stg.contactid,
                      'F' AS communication_device_type_code,
                      trim(cardioaccess_contacts_stg.faxnumber) AS communication_device_num,
                      cardioaccess_contacts_stg.full_server_nm
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
                ) AS unions
              WHERE unions.communication_device_num IS NOT NULL
          ) AS wrk0
          INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_communication_device AS cad ON upper(wrk0.communication_device_num) = upper(cad.communication_device_num)
    ) AS b
;
