-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_provider_service_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT remittance_service.ref_idn_qualifier1 AS provider_serv_id_qlfr_code,
          remittance_service.provider_identifier1 AS provider_serv_id,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier1, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier1, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier2 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier2 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier2, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier2, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier3 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier3 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier3, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier3, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier4 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier4 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier4, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier4, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier5 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier5 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier5, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier5, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier6 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier6 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier6, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier6, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier7 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier7 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier7, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier7, '')) <> ''
   UNION DISTINCT SELECT remittance_service.ref_idn_qualifier8 AS provider_serv_id_qlfr_code,
                         remittance_service.provider_identifier8 AS provider_serv_id,
                         'E' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.ref_idn_qualifier8, '')) <> ''
     OR upper(coalesce(remittance_service.provider_identifier8, '')) <> '' ) AS a 