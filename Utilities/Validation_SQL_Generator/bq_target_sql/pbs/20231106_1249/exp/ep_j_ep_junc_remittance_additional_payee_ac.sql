-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_junc_remittance_additional_payee_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          f.payment_guid,
          f.additional_payee_line_num,
          rrap.remittance_additional_payee_sid AS remittance_additional_payee_sid,
          --  Need to join with REF_REMITTANCE_ADDITIONAL_PAYEE for this sid column
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                remittance_payment.payment_guid,
                remittance_payment.additional_payee1_id AS additional_payee_id,
                remittance_payment.reference_id_qualifier1_code AS additional_payee_id_qualifier_code,
                1 AS additional_payee_line_num
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier1_code, '')) <> ''
               OR upper(coalesce(remittance_payment.additional_payee1_id, '')) <> ''
            UNION DISTINCT
            SELECT
                remittance_payment.payment_guid,
                remittance_payment.additional_payee2_id AS additional_payee_id,
                remittance_payment.reference_id_qualifier2_code AS additional_payee_id_qualifier_code,
                2 AS additional_payee_line_num
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier2_code, '')) <> ''
               OR upper(coalesce(remittance_payment.additional_payee2_id, '')) <> ''
            UNION DISTINCT
            SELECT
                remittance_payment.payment_guid,
                remittance_payment.additional_payee3_id AS additional_payee_id,
                remittance_payment.reference_id_qualifier3_code AS additional_payee_id_qualifier_code,
                3 AS additional_payee_line_num
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier3_code, '')) <> ''
               OR upper(coalesce(remittance_payment.additional_payee3_id, '')) <> ''
            UNION DISTINCT
            SELECT
                remittance_payment.payment_guid,
                remittance_payment.additional_payee4_id AS additional_payee_id,
                remittance_payment.reference_id_qualifier4_code AS additional_payee_id_qualifier_code,
                4 AS additional_payee_line_num
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_payment
              WHERE upper(coalesce(remittance_payment.reference_id_qualifier4_code, '')) <> ''
               OR upper(coalesce(remittance_payment.additional_payee4_id, '')) <> ''
          ) AS f
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_additional_payee AS rrap ON upper(f.additional_payee_id) = upper(rrap.additional_payee_id)
           AND upper(f.additional_payee_id_qualifier_code) = upper(rrap.additional_payee_id_qualifier_code)
    ) AS a
;
