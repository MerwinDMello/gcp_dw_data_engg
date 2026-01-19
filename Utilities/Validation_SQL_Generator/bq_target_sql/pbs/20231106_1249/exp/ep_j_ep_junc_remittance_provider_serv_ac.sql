-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_junc_remittance_provider_serv_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          f.service_guid,
          f.provider_serv_id_line_num,
          coalesce(rps.remittance_provider_serv_sid, CAST(99999 as NUMERIC)) AS remittance_provider_serv_sid,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          (
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                1 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier1 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier1 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier1, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier1, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                2 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier2 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier2 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier2, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier2, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                3 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier3 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier3 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier3, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier3, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                4 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier4 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier4 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier4, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier4, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                5 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier5 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier5 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier5, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier5, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                6 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier6 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier6 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier6, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier6, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                7 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier7 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier7 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier7, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier7, '')) NOT IN(
                ''
              ))
            UNION ALL
            SELECT
                remittance_service.service_guid,
                remittance_service.claim_guid,
                8 AS provider_serv_id_line_num,
                remittance_service.ref_idn_qualifier8 AS provider_serv_id_qlfr_code,
                remittance_service.provider_identifier8 AS provider_serv_id
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
              WHERE upper(remittance_service.delete_ind) = 'N'
               AND (upper(coalesce(remittance_service.ref_idn_qualifier8, '')) NOT IN(
                ''
              )
               OR upper(coalesce(remittance_service.provider_identifier8, '')) NOT IN(
                ''
              ))
          ) AS f
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_provider_service AS rps ON upper(rps.provider_serv_id_qlfr_code) = upper(f.provider_serv_id_qlfr_code)
           AND upper(rps.provider_serv_id) = upper(f.provider_serv_id)
    ) AS a
;
