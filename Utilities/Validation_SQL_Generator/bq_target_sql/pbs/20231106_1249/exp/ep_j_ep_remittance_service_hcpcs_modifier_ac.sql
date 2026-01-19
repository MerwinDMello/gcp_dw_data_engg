-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_remittance_service_hcpcs_modifier_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          remittance_service.service_guid AS service_guid,
          'A' AS hcpcs_type_ind,
          3 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_1_code3 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_1_code3, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'A' AS hcpcs_type_ind,
          4 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_1_code4 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_1_code4, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'A' AS hcpcs_type_ind,
          5 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_1_code5 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_1_code5, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'A' AS hcpcs_type_ind,
          6 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_1_code6 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_1_code6, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'O' AS hcpcs_type_ind,
          3 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_6_code3 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_6_code3, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'O' AS hcpcs_type_ind,
          4 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_6_code4 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_6_code4, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'O' AS hcpcs_type_ind,
          5 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_6_code5 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_6_code5, '')) NOT IN(
          ''
        )
      UNION DISTINCT
      SELECT
          remittance_service.service_guid AS service_guid,
          'O' AS hcpcs_type_ind,
          6 AS hcpcs_modifier_seq_num,
          remittance_service.procedure_modifier_6_code6 AS hcpcs_modifier_code,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
        WHERE upper(coalesce(remittance_service.procedure_modifier_6_code6, '')) NOT IN(
          ''
        )
    ) AS a
;
