-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_junc_remittance_secn_rendering_prov_service_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT remittance_service.claim_guid,
          'NA' AS payment_guid,
          remittance_service.service_guid,
          remittance_service.rendrng_provdr_ref_idn_qual1 AS secn_rendering_provider_id_qlfr_code,
          remittance_service.rendering_provider_identifier1 AS secn_rendering_provider_id,
          1 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual1, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier1, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual2 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier2 AS secn_rendering_provider_id,
                         2 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual2, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier2, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual3 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier3 AS secn_rendering_provider_id,
                         3 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual3, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier3, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual4 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier4 AS secn_rendering_provider_id,
                         4 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual4, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier4, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual5 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier5 AS secn_rendering_provider_id,
                         5 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual5, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier5, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual6 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier6 AS secn_rendering_provider_id,
                         6 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual6, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier6, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual7 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier7 AS secn_rendering_provider_id,
                         7 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual7, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier7, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual8 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier8 AS secn_rendering_provider_id,
                         8 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual8, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier8, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual9 AS secn_rendering_provider_id_qlfr_code,
                         remittance_service.rendering_provider_identifier9 AS secn_rendering_provider_id,
                         9 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual9, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_provider_identifier9, '')) NOT IN('')
   UNION DISTINCT SELECT remittance_service.claim_guid,
                         'NA' AS payment_guid,
                         remittance_service.service_guid,
                         remittance_service.rendrng_provdr_ref_idn_qual10 AS secn_rendering_provider_id_qlfr_code,
                         substr(remittance_service.rendering_providr_identifier10, 1, 100) AS secn_rendering_provider_id,
                         10 AS secn_rendering_provider_id_line_num
   FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service
   WHERE upper(coalesce(remittance_service.rendrng_provdr_ref_idn_qual10, '')) NOT IN('')
     OR upper(coalesce(remittance_service.rendering_providr_identifier10, '')) NOT IN('') ) AS f
LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_secn_remittance_rendering_provider AS rrap ON upper(f.secn_rendering_provider_id) = upper(rrap.secn_rendering_provider_id)
AND upper(f.secn_rendering_provider_id_qlfr_code) = upper(rrap.secn_rendering_provider_id_qlfr_code) 