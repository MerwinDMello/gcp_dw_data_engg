-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_payee_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_payee.remittance_payee_sid), 0)
      FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payee) + row_number() OVER (
                                                                                        ORDER BY provider_npi,
                                                                                                 stg.provider_tax_id,
                                                                                                 upper(max(stg.payee_name)),
                                                                                                 upper(max(stg.payee_identification_qual_code)),
                                                                                                 upper(max(stg.payee_city_name)),
                                                                                                 upper(max(stg.payee_state_code)),
                                                                                                 upper(max(stg.payee_postal_zone_code)),
                                                                                                 upper(max(stg.provider_tax_id_lookup_code))) AS remittance_payee_sid, /*
          CASE WHEN Length(STG.PROVIDER_TAX_ID ) = 10 THEN STG.PROVIDER_TAX_ID ELSE NULL end AS Provider_NPI,
           CASE WHEN Length(STG.PROVIDER_TAX_ID ) = 9 THEN STG.PROVIDER_TAX_ID
           WHEN Length(Cast(STG.PROVIDER_TAX_ID AS VARCHAR(100) )) = 8 THEN '0'||STG.PROVIDER_TAX_ID
           */ provider_npi,
              CASE length(trim(format('%11d', stg.provider_tax_id)))
                  WHEN 9 THEN trim(format('%11d', stg.provider_tax_id))
                  WHEN 8 THEN concat('0', trim(format('%11d', stg.provider_tax_id)))
                  ELSE CAST(NULL AS STRING)
              END AS provider_tax_id,
              max(stg.payee_name) AS payee_name,
              max(stg.payee_identification_qual_code) AS payee_identification_qualifier_code,
              max(stg.payee_city_name) AS payee_city_name,
              max(stg.payee_state_code) AS payee_state_code,
              max(stg.payee_postal_zone_code) AS payee_postal_zone_code,
              'E' AS source_system_code,
              timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
              max(stg.provider_tax_id_lookup_code) AS provider_tax_id_lookup_code
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment AS stg
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN length(trim(format('%11d', stg.provider_tax_id))) = 10 THEN trim(format('%11d', stg.provider_tax_id))
                                ELSE CAST(NULL AS STRING)
                            END ]) AS provider_npi
   WHERE DATE(stg.dw_last_update_date_time) =
       (SELECT max(DATE(remittance_payment.dw_last_update_date_time))
        FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment)
     AND (upper(stg.payee_name),
          stg.payee_identification_qual_code,
          upper(stg.payee_city_name),
          upper(stg.payee_state_code),
          upper(stg.payee_postal_zone_code),
          upper(stg.provider_tax_id_lookup_code),
          CASE
              WHEN length(trim(format('%11d', stg.provider_tax_id))) = 10 THEN trim(format('%11d', stg.provider_tax_id))
              ELSE ''
          END,
          CASE length(trim(format('%11d', stg.provider_tax_id)))
              WHEN 9 THEN trim(format('%11d', stg.provider_tax_id))
              WHEN 8 THEN concat('0', trim(format('%11d', stg.provider_tax_id)))
              ELSE ''
          END) NOT IN
       (SELECT DISTINCT AS STRUCT upper(a.payee_name) AS payee_name,
                                  upper(a.payee_identification_qualifier_code) AS payee_identification_qualifier_code,
                                  upper(a.payee_city_name) AS payee_city_name,
                                  upper(a.payee_state_code) AS payee_state_code,
                                  upper(a.payee_postal_zone_code) AS payee_postal_zone_code,
                                  upper(a.provider_tax_id_lookup_code) AS provider_tax_id_lookup_code,
                                  coalesce(a.provider_npi, ''),
                                  coalesce(a.provider_tax_id, '')
        FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payee AS a)
   GROUP BY 2,
            stg.provider_tax_id,
            upper(stg.payee_name),
            upper(stg.payee_identification_qual_code),
            upper(stg.payee_city_name),
            upper(stg.payee_state_code),
            upper(stg.payee_postal_zone_code),
            upper(stg.provider_tax_id_lookup_code)) AS a 