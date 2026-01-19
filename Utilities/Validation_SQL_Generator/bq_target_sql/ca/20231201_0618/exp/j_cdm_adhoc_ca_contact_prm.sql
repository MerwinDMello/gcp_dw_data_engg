-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_contact_prm.sql
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
                NULL AS contact_sk,
                a_0.contact_type_sk AS contact_type_sk,
                addr.address_sk AS address_sk,
                ser.server_sk AS server_sk,
                stg.contactid AS source_contact_id,
                stg.firstname AS contact_first_name,
                stg.middlename AS contact_middle_name,
                stg.lastname AS contact_last_name,
                stg.suffix AS contact_suffix_name,
                stg.emailname AS email_address_text,
                stg.companyname AS company_name,
                stg.notes AS note_text,
                CAST(trim(stg.dateentered) as DATETIME) AS contact_effective_from_date,
                CAST(trim(stg.inactive) as DATETIME) AS contact_effective_to_date,
                stg.createdate AS source_create_date_time,
                stg.lastupdate AS source_last_update_date_time,
                stg.updatedby AS updated_by_3_4_id,
                'C' AS source_system_code,
                timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
              FROM
                `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contacts_stg AS stg
                LEFT OUTER JOIN (
                  SELECT
                      c.contact_type_sk,
                      c.source_contact_type_id,
                      s.server_name
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm.ca_contact_type AS c
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON c.server_sk = s.server_sk
                ) AS a_0 ON stg.contacttype = a_0.source_contact_type_id
                 AND upper(stg.full_server_nm) = upper(a_0.server_name)
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_views.ref_ca_global_lookup AS cglus ON upper(stg.country) = upper(cglus.sts_code_text)
                 AND upper(cglus.short_name) = 'ISOCOUNTRY'
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_address AS addr ON upper(stg.address) = upper(addr.address_line_1_text)
                 AND upper(stg.address2) = upper(addr.address_line_2_text)
                 AND upper(stg.address3) = upper(addr.address_line_3_text)
                 AND upper(stg.city) = upper(addr.city_name)
                 AND upper(stg.stateorprovince) = upper(addr.state_name)
                 AND upper(stg.postalcode) = upper(addr.zip_code)
                 AND upper(stg.county) = upper(addr.county_name)
                 AND cglus.lookup_id = addr.country_id
                INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_contact AS ch ON ch.server_sk = ser.server_sk
                 AND ch.source_contact_id = stg.contactid
              WHERE ch.server_sk IS NULL
               AND ch.source_contact_id IS NULL
          ) AS a
    ) AS b
;
