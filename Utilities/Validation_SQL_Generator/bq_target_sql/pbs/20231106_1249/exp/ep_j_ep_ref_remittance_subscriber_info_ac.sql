-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_subscriber_info_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*) + 1) AS source_string
  FROM
    (
      SELECT
          (
            SELECT
                coalesce(max(ref_remittance_subscriber_info.remittance_subscriber_sid), 0)
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_subscriber_info
          ) + row_number() OVER (ORDER BY upper(f.patient_health_ins_num), upper(f.insured_identification_qualifier_code), upper(f.subscriber_id), upper(f.insured_entity_type_qualifier_code), upper(f.subscriber_last_name), upper(f.subscriber_first_name), upper(f.subscriber_middle_name), upper(f.subscriber_name_suffix)) AS remittance_subscriber_sid,
          --  SID
          f.patient_health_ins_num,
          f.insured_identification_qualifier_code,
          f.subscriber_id,
          f.insured_entity_type_qualifier_code,
          f.subscriber_last_name,
          f.subscriber_first_name,
          f.subscriber_middle_name,
          f.subscriber_name_suffix,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                max(remittance_claim.ins_subscriber_id) AS patient_health_ins_num,
                max(remittance_claim.insurd_identifictn_qualifr_cod) AS insured_identification_qualifier_code,
                max(remittance_claim.subscriber_id) AS subscriber_id,
                max(remittance_claim.insured_entity_typ_qualifr_cod) AS insured_entity_type_qualifier_code,
                max(remittance_claim.subscriber_last_name) AS subscriber_last_name,
                max(remittance_claim.subscriber_first_name) AS subscriber_first_name,
                max(remittance_claim.subscriber_middle_name) AS subscriber_middle_name,
                max(remittance_claim.subscriber_name_suffix) AS subscriber_name_suffix
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
              WHERE DATE(remittance_claim.dw_last_update_date_time) = (
                SELECT
                    max(DATE(remittance_claim_0.dw_last_update_date_time))
                  FROM
                    `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim AS remittance_claim_0
              )
               AND upper(coalesce(remittance_claim.ins_subscriber_id, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.insurd_identifictn_qualifr_cod, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.subscriber_id, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.insured_entity_typ_qualifr_cod, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.subscriber_last_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.subscriber_first_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.subscriber_middle_name, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_claim.subscriber_name_suffix, '')) NOT IN(
                ''
              )
              GROUP BY upper(remittance_claim.ins_subscriber_id), upper(remittance_claim.insurd_identifictn_qualifr_cod), upper(remittance_claim.subscriber_id), upper(remittance_claim.insured_entity_typ_qualifr_cod), upper(remittance_claim.subscriber_last_name), upper(remittance_claim.subscriber_first_name), upper(remittance_claim.subscriber_middle_name), upper(remittance_claim.subscriber_name_suffix)
          ) AS f
        WHERE (upper(f.patient_health_ins_num), upper(f.insured_identification_qualifier_code), upper(f.subscriber_id), upper(f.insured_entity_type_qualifier_code), upper(f.subscriber_last_name), upper(f.subscriber_first_name), upper(f.subscriber_middle_name), upper(f.subscriber_name_suffix)) NOT IN(
          SELECT AS STRUCT
              upper(ref_remittance_subscriber_info.patient_health_ins_num) AS patient_health_ins_num,
              upper(ref_remittance_subscriber_info.insured_identification_qualifier_code) AS insured_identification_qualifier_code,
              upper(ref_remittance_subscriber_info.subscriber_id) AS subscriber_id,
              upper(ref_remittance_subscriber_info.insured_entity_type_qualifier_code) AS insured_entity_type_qualifier_code,
              upper(ref_remittance_subscriber_info.subscriber_last_name) AS subscriber_last_name,
              upper(ref_remittance_subscriber_info.subscriber_first_name) AS subscriber_first_name,
              upper(ref_remittance_subscriber_info.subscriber_middle_name) AS subscriber_middle_name,
              upper(ref_remittance_subscriber_info.subscriber_name_suffix) AS subscriber_name_suffix
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_subscriber_info
        )
    ) AS a
;
