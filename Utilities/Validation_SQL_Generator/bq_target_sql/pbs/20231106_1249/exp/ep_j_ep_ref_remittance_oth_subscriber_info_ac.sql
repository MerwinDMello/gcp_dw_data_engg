-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_oth_subscriber_info_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*) + 1) AS source_string
  FROM
    (
      SELECT
          (
            SELECT
                coalesce(max(ref_remittance_oth_subscriber_info.remittance_oth_subscriber_sid), 0)
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_oth_subscriber_info
          ) + row_number() OVER (ORDER BY upper(f.oth_subscriber_id_qualifier_code), upper(f.oth_subscriber_id), upper(f.oth_subscriber_enty_type_qualifier_code), upper(f.oth_subscriber_last_name), upper(f.oth_subscriber_first_name), upper(f.oth_subscriber_middle_name), upper(f.oth_subscriber_name_suffix)) AS remittance_oth_subscriber_sid,
          --  SID
          f.oth_subscriber_id_qualifier_code,
          f.oth_subscriber_id,
          f.oth_subscriber_enty_type_qualifier_code,
          f.oth_subscriber_last_name,
          f.oth_subscriber_first_name,
          f.oth_subscriber_middle_name,
          f.oth_subscriber_name_suffix,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                max(remittance_claim.othr_subcbr_enty_typ_qulfr_cod) AS oth_subscriber_enty_type_qualifier_code,
                max(remittance_claim.other_subscriber_last_name) AS oth_subscriber_last_name,
                max(remittance_claim.other_subscriber_first_name) AS oth_subscriber_first_name,
                max(remittance_claim.other_subscriber_middle_name) AS oth_subscriber_middle_name,
                max(remittance_claim.other_subscriber_name_suffix) AS oth_subscriber_name_suffix,
                max(remittance_claim.othr_subcbr_idntfctn_qulfr_cod) AS oth_subscriber_id_qualifier_code,
                max(remittance_claim.other_subscriber_id) AS oth_subscriber_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
              WHERE DATE(remittance_claim.dw_last_update_date_time) = (
                SELECT
                    max(DATE(remittance_claim_0.dw_last_update_date_time))
                  FROM
                    `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim AS remittance_claim_0
              )
               AND upper(coalesce(remittance_claim.othr_subcbr_enty_typ_qulfr_cod, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.other_subscriber_last_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.other_subscriber_first_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.other_subscriber_middle_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.other_subscriber_name_suffix, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.othr_subcbr_idntfctn_qulfr_cod, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.other_subscriber_id, '')) NOT IN(
                ''
              )
              GROUP BY upper(remittance_claim.othr_subcbr_enty_typ_qulfr_cod), upper(remittance_claim.other_subscriber_last_name), upper(remittance_claim.other_subscriber_first_name), upper(remittance_claim.other_subscriber_middle_name), upper(remittance_claim.other_subscriber_name_suffix), upper(remittance_claim.othr_subcbr_idntfctn_qulfr_cod), upper(remittance_claim.other_subscriber_id)
          ) AS f
        WHERE (upper(f.oth_subscriber_id_qualifier_code), upper(f.oth_subscriber_id), upper(f.oth_subscriber_enty_type_qualifier_code), upper(f.oth_subscriber_last_name), upper(f.oth_subscriber_first_name), upper(f.oth_subscriber_middle_name), upper(f.oth_subscriber_name_suffix)) NOT IN(
          SELECT AS STRUCT
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_id_qualifier_code) AS oth_subscriber_id_qualifier_code,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_id) AS oth_subscriber_id,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_enty_type_qualifier_code) AS oth_subscriber_enty_type_qualifier_code,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_last_name) AS oth_subscriber_last_name,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_first_name) AS oth_subscriber_first_name,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_middle_name) AS oth_subscriber_middle_name,
              upper(ref_remittance_oth_subscriber_info.oth_subscriber_name_suffix) AS oth_subscriber_name_suffix
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_oth_subscriber_info
        )
    ) AS a
;
