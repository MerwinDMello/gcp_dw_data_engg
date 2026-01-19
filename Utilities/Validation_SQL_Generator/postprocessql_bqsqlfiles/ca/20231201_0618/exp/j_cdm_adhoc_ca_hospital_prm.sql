-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_hospital_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT DISTINCT NULL AS hospital_sk,
                      ca.address_sk AS hospital_address_sk,
                      csrv.server_sk AS server_sk,
                      chs.hospitalid AS source_hospital_id,
                      chs.organizationid AS organization_id,
                      chs.hospname AS hospital_name,
                      chs.hospnpi AS hospital_npi_text,
                      chs.createdate AS source_create_date_time,
                      chs.lastupdate AS source_last_update_date_time,
                      chs.updatedby AS updated_by_3_4_id,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_hospital_stg AS chs
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(chs.full_server_nm) = upper(csrv.server_name)
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_views.ref_ca_global_lookup AS cglus ON upper(coalesce(chs.hospcountry, ' ')) = upper(coalesce(cglus.sts_code_text, ' '))
      AND upper(cglus.short_name) = 'ISOCOUNTRY'
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_address AS ca ON upper(coalesce(chs.hospaddress, ' ')) = upper(coalesce(ca.address_line_1_text, ' '))
      AND upper(coalesce(chs.hospaddress2, ' ')) = upper(coalesce(ca.address_line_2_text, ' '))
      AND upper(coalesce(chs.hospcity, ' ')) = upper(coalesce(ca.city_name, ' '))
      AND upper(coalesce(chs.hospstate, ' ')) = upper(coalesce(ca.state_name, ' '))
      AND upper(coalesce(chs.hospzip, ' ')) = upper(coalesce(ca.zip_code, ' '))
      AND coalesce(cglus.lookup_id, ' ') = coalesce(format('%11d', ca.country_id), ' ')
      AND ca.county_name IS NULL
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_hospital AS ch ON ch.server_sk = csrv.server_sk
      AND ch.source_hospital_id = chs.hospitalid
      WHERE ch.server_sk IS NULL
        AND ch.source_hospital_id IS NULL ) AS a) AS b