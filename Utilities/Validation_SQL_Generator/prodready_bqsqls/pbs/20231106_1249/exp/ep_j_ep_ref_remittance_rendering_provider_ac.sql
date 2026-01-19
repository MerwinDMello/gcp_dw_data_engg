-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_rendering_provider_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*) + 1) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_rendering_provider.remittance_rendering_provider_sid), 0)
      FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY upper(f.serv_provider_enty_type_qualifier_code),
                                                                                                         upper(f.rendering_provider_last_org_name),
                                                                                                         upper(f.rendering_provider_first_name),
                                                                                                         upper(f.rendering_provider_middle_name),
                                                                                                         upper(f.rendering_provider_name_suffix),
                                                                                                         upper(f.serv_provider_id_qualifier_code),
                                                                                                         upper(f.rendering_provider_id)) AS remittance_rendering_provider_sid, --  SID
 f.serv_provider_enty_type_qualifier_code,
 f.rendering_provider_last_org_name,
 f.rendering_provider_first_name,
 f.rendering_provider_middle_name,
 f.rendering_provider_name_suffix,
 f.serv_provider_id_qualifier_code,
 f.rendering_provider_id,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT max(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod) AS serv_provider_enty_type_qualifier_code,
             max(remittance_claim.rendering_provider_last_org_nm) AS rendering_provider_last_org_name,
             max(remittance_claim.rendering_provider_first_name) AS rendering_provider_first_name,
             max(remittance_claim.rendering_provider_middle_name) AS rendering_provider_middle_name,
             max(remittance_claim.rendering_provider_name_suffix) AS rendering_provider_name_suffix,
             max(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod) AS serv_provider_id_qualifier_code,
             max(remittance_claim.rendering_provider_id) AS rendering_provider_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE DATE(remittance_claim.dw_last_update_date_time) =
          (SELECT max(DATE(remittance_claim_0.dw_last_update_date_time))
           FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim AS remittance_claim_0)
        AND upper(coalesce(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_last_org_nm, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_first_name, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_middle_name, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_name_suffix, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provider_id, '')) NOT IN('')
      GROUP BY upper(remittance_claim.srvce_prvdr_enty_typ_qulfr_cod),
               upper(remittance_claim.rendering_provider_last_org_nm),
               upper(remittance_claim.rendering_provider_first_name),
               upper(remittance_claim.rendering_provider_middle_name),
               upper(remittance_claim.rendering_provider_name_suffix),
               upper(remittance_claim.srvce_prvdr_idntfctn_qulfr_cod),
               upper(remittance_claim.rendering_provider_id)) AS f
   WHERE (upper(coalesce(f.serv_provider_enty_type_qualifier_code, '')),
          upper(coalesce(f.rendering_provider_last_org_name, '')),
          upper(coalesce(f.rendering_provider_first_name, '')),
          upper(coalesce(f.rendering_provider_middle_name, '')),
          upper(coalesce(f.rendering_provider_name_suffix, '')),
          upper(coalesce(f.serv_provider_id_qualifier_code, '')),
          upper(coalesce(f.rendering_provider_id, ''))) NOT IN
       (SELECT AS STRUCT upper(coalesce(ref_remittance_rendering_provider.serv_provider_enty_type_qualifier_code, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_last_org_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_first_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_middle_name, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_name_suffix, '')),
                         upper(coalesce(ref_remittance_rendering_provider.serv_provider_id_qualifier_code, '')),
                         upper(coalesce(ref_remittance_rendering_provider.rendering_provider_id, ''))
        FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_rendering_provider
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) ) AS a