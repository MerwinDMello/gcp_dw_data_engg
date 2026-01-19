-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_payor_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          (
            SELECT
                coalesce(max(ref_remittance_payor.remittance_payor_sid), 0)
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payor
          ) + row_number() OVER (ORDER BY upper(max(stg.payment_carrier_num)), upper(max(stg.payment_agency_num)), upper(max(stg.payor_ref_id)), upper(max(stg.payor_name)), upper(max(stg.payor_city_name)), upper(max(stg.payor_state_code)), upper(max(stg.payor_postal_zone_code))) AS remittance_payor_sid,
          max(stg.payment_carrier_num) AS payment_carrier_num,
          -- EP_Payor_Num ,
          max(stg.payment_agency_num) AS payment_agency_num,
          max(stg.payor_ref_id) AS payor_ref_id,
          max(stg.payor_name) AS payor_name,
          max(stg.payor_address_line_1) AS payor_address_line_1,
          max(stg.payor_address_line_2) AS payor_address_line_2,
          max(stg.payor_city_name) AS payor_city_name,
          max(stg.payor_state_code) AS payor_state_code,
          max(stg.payor_postal_zone_code) AS payor_postal_zone_code,
          max(stg.payor_line_of_business) AS payor_line_of_business,
          max(stg.payor_alternate_ref_id) AS payor_alternate_ref_id,
          max(stg.payor_long_name) AS payor_long_name,
          max(stg.payor_short_name) AS payor_short_name,
          /*Payor_Technical_Contact_Name ,
          Payor_Primary_Comm_Type_Code ,
          Payor_Primary_Contact_Comm_Num ,
          Payor_Secondary_Comm_Type_Code,
          Payor_Secondary_Contact_Comm_Num,
          Payor_Tertiary_Comm_Type_Code ,
          Payor_Tertiary_Contact_Comm_Num,*/
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment AS stg
        WHERE DATE(stg.dw_last_update_date_time) = (
          SELECT
              max(DATE(remittance_payment.dw_last_update_date_time))
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
        )
         AND (upper(stg.payment_carrier_num), upper(stg.payment_agency_num), upper(stg.payor_ref_id), upper(stg.payor_name), upper(stg.payor_address_line_1), upper(stg.payor_address_line_2), upper(stg.payor_city_name), upper(stg.payor_state_code), upper(stg.payor_postal_zone_code), upper(stg.payor_line_of_business), upper(stg.payor_alternate_ref_id), upper(stg.payor_long_name), upper(stg.payor_short_name)) NOT IN(
          SELECT AS STRUCT
              upper(ref_remittance_payor.payment_carrier_num) AS payment_carrier_num,
              upper(ref_remittance_payor.payment_agency_num_an) AS payment_agency_num_an,
              upper(ref_remittance_payor.payor_ref_id) AS payor_ref_id,
              upper(ref_remittance_payor.payor_name) AS payor_name,
              upper(ref_remittance_payor.payor_address_line_1) AS payor_address_line_1,
              upper(ref_remittance_payor.payor_address_line_2) AS payor_address_line_2,
              upper(ref_remittance_payor.payor_city_name) AS payor_city_name,
              upper(ref_remittance_payor.payor_state_code) AS payor_state_code,
              upper(ref_remittance_payor.payor_postal_zone_code) AS payor_postal_zone_code,
              upper(ref_remittance_payor.payor_line_of_business) AS payor_line_of_business,
              upper(ref_remittance_payor.payor_alternate_ref_id) AS payor_alternate_ref_id,
              upper(ref_remittance_payor.payor_long_name) AS payor_long_name,
              upper(ref_remittance_payor.payor_short_name) AS payor_short_name
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payor
        )
        GROUP BY upper(stg.payment_carrier_num), upper(stg.payment_agency_num), upper(stg.payor_ref_id), upper(stg.payor_name), upper(stg.payor_address_line_1), upper(stg.payor_address_line_2), upper(stg.payor_city_name), upper(stg.payor_state_code), upper(stg.payor_postal_zone_code), upper(stg.payor_line_of_business), upper(stg.payor_alternate_ref_id), upper(stg.payor_long_name), upper(stg.payor_short_name)
    ) AS a
;
-- EP_Payor_Num ,
