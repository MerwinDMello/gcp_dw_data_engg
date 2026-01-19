-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_payor_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_payor_address AS SELECT
    payment_compliance_payor_address.address_sid,
    payment_compliance_payor_address.major_payor_group_id,
    payment_compliance_payor_address.salutation_name,
    payment_compliance_payor_address.payor_name,
    payment_compliance_payor_address.address_1_text,
    payment_compliance_payor_address.address_2_text,
    payment_compliance_payor_address.city_name,
    payment_compliance_payor_address.state_code,
    payment_compliance_payor_address.zip_code,
    payment_compliance_payor_address.fax_num,
    payment_compliance_payor_address.email_text,
    payment_compliance_payor_address.eff_start_date,
    payment_compliance_payor_address.eff_end_date,
    payment_compliance_payor_address.source_system_code,
    payment_compliance_payor_address.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payment_compliance_payor_address
;
