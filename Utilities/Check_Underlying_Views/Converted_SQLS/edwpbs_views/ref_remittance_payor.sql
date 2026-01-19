-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_payor
   OPTIONS(description='Reference table to maintain the Payor details for the payments received.')
  AS SELECT
      a.remittance_payor_sid,
      a.payment_carrier_num,
      a.ep_payor_num,
      a.payment_agency_num_an,
      a.payor_ref_id,
      a.payor_name,
      a.payor_address_line_1,
      a.payor_address_line_2,
      a.payor_city_name,
      a.payor_state_code,
      a.payor_postal_zone_code,
      a.payor_line_of_business,
      a.payor_alternate_ref_id,
      a.payor_long_name,
      a.payor_short_name,
      a.payor_technical_contact_name,
      a.payor_primary_comm_type_code,
      a.payor_primary_contact_comm_num,
      a.payor_secondary_comm_type_code,
      a.payor_secondary_contact_comm_num,
      a.payor_tertiary_comm_type_code,
      a.payor_tertiary_contact_comm_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_payor AS a
  ;
