-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_additional_payee_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          (
            SELECT
                coalesce(max(ref_remittance_additional_payee.remittance_additional_payee_sid), 0)
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_additional_payee
          ) + row_number() OVER (ORDER BY upper(f.additional_payee_id_qualifier_code), upper(f.additional_payee_id)) AS remittance_additional_payee_sid,
          --  SID
          f.additional_payee_id_qualifier_code,
          f.additional_payee_id,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                remittance_payment.reference_id_qualifier1_code AS additional_payee_id_qualifier_code,
                remittance_payment.additional_payee1_id AS additional_payee_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier1_code, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_payment.additional_payee1_id, '')) NOT IN(
                ''
              )
            UNION DISTINCT
            SELECT
                remittance_payment.reference_id_qualifier2_code AS additional_payee_id_qualifier_code,
                remittance_payment.additional_payee2_id AS additional_payee_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier2_code, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_payment.additional_payee2_id, '')) NOT IN(
                ''
              )
            UNION DISTINCT
            SELECT
                remittance_payment.reference_id_qualifier3_code AS additional_payee_id_qualifier_code,
                remittance_payment.additional_payee3_id AS additional_payee_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier3_code, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_payment.additional_payee3_id, '')) NOT IN(
                ''
              )
            UNION DISTINCT
            SELECT
                remittance_payment.reference_id_qualifier4_code AS additional_payee_id_qualifier_code,
                remittance_payment.additional_payee4_id AS additional_payee_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier4_code, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_payment.additional_payee4_id, '')) NOT IN(
                ''
              )
          ) AS f
    ) AS a
;
