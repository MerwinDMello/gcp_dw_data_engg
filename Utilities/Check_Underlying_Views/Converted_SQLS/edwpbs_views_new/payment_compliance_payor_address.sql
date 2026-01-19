-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_payor_address.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_payor_address AS SELECT
    a.address_sid,
    a.major_payor_group_id,
    a.salutation_name,
    a.payor_name,
    a.address_1_text,
    a.address_2_text,
    a.city_name,
    a.state_code,
    a.zip_code,
    a.fax_num,
    a.email_text,
    a.eff_start_date,
    a.eff_end_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_payor_address AS a
;
