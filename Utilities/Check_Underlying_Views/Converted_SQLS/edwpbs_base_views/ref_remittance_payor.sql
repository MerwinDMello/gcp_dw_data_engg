-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_payor
   OPTIONS(description='Reference table to maintain the Payor details for the payments received.')
  AS SELECT
      ref_remittance_payor.remittance_payor_sid,
      ref_remittance_payor.payment_carrier_num,
      ref_remittance_payor.ep_payor_num,
      ref_remittance_payor.payment_agency_num_an,
      ref_remittance_payor.payor_ref_id,
      ref_remittance_payor.payor_name,
      ref_remittance_payor.payor_address_line_1,
      ref_remittance_payor.payor_address_line_2,
      ref_remittance_payor.payor_city_name,
      ref_remittance_payor.payor_state_code,
      ref_remittance_payor.payor_postal_zone_code,
      ref_remittance_payor.payor_line_of_business,
      ref_remittance_payor.payor_alternate_ref_id,
      ref_remittance_payor.payor_long_name,
      ref_remittance_payor.payor_short_name,
      ref_remittance_payor.payor_technical_contact_name,
      ref_remittance_payor.payor_primary_comm_type_code,
      ref_remittance_payor.payor_primary_contact_comm_num,
      ref_remittance_payor.payor_secondary_comm_type_code,
      ref_remittance_payor.payor_secondary_contact_comm_num,
      ref_remittance_payor.payor_tertiary_comm_type_code,
      ref_remittance_payor.payor_tertiary_contact_comm_num,
      ref_remittance_payor.source_system_code,
      ref_remittance_payor.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_payor
  ;
