-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_communication_device_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT unions.communication_device_num
   FROM
     (SELECT CASE
                 WHEN cardioaccess_contacts_stg.workextension IS NOT NULL THEN concat(trim(cardioaccess_contacts_stg.workphone), ':', trim(cardioaccess_contacts_stg.workextension))
                 ELSE trim(cardioaccess_contacts_stg.workphone)
             END AS communication_device_num,
             cardioaccess_contacts_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
      UNION DISTINCT SELECT trim(cardioaccess_contacts_stg.pagernum) AS communication_device_num,
                            cardioaccess_contacts_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
      UNION DISTINCT SELECT trim(cardioaccess_contacts_stg.homephone) AS communication_device_num,
                            cardioaccess_contacts_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
      UNION DISTINCT SELECT trim(cardioaccess_contacts_stg.mobilephone) AS communication_device_num,
                            cardioaccess_contacts_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
      UNION DISTINCT SELECT trim(cardioaccess_contacts_stg.faxnumber) AS communication_device_num,
                            cardioaccess_contacts_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg
      UNION DISTINCT SELECT trim(cardioaccess_demographics_stg.patphone) AS communication_device_num,
                            cardioaccess_demographics_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
      UNION DISTINCT SELECT trim(cardioaccess_demographics_stg.patfax) AS communication_device_num,
                            cardioaccess_demographics_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
      UNION DISTINCT SELECT trim(cardioaccess_demographics_stg.patwphone) AS communication_device_num,
                            cardioaccess_demographics_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
      UNION DISTINCT SELECT trim(cardioaccess_demographics_stg.patcphone) AS communication_device_num,
                            cardioaccess_demographics_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg
      UNION DISTINCT SELECT trim(cardioaccess_demographics_stg.pager) AS communication_device_num,
                            cardioaccess_demographics_stg.full_server_nm
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_demographics_stg) AS unions
   WHERE unions.communication_device_num IS NOT NULL
     AND NOT EXISTS
       (SELECT 1
        FROM `hca-hin-dev-cur-clinical`.edwcdm.ca_communication_device AS tgt
        WHERE upper(tgt.communication_device_num) = upper(unions.communication_device_num) ) ) AS a