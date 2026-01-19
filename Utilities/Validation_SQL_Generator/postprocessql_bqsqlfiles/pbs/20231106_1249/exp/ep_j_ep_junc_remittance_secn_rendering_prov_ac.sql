-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_junc_remittance_secn_rendering_prov_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT f.claim_guid,
          f.payment_guid,
          f.secn_rendering_provider_id_line_num,
          rrap.remittance_secn_rendering_provider_sid, -- RRAP.Remittance_Secn_Rendering_Provider_SID AS Remittance_Secn_Rendering_Provider_SID ,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_claim.claim_guid,
             remittance_claim.payment_guid,
             remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod AS secn_rendering_provider_id_qlfr_code,
             remittance_claim.rendering_provder_secndary_id1 AS secn_rendering_provider_id,
             1 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id1, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id2 AS secn_rendering_provider_id,
                            2 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id2, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id3 AS secn_rendering_provider_id,
                            3 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id3, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id4 AS secn_rendering_provider_id,
                            4 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id4, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id5 AS secn_rendering_provider_id,
                            5 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id5, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id6 AS secn_rendering_provider_id,
                            6 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id6, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id7 AS secn_rendering_provider_id,
                            7 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id7, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id8 AS secn_rendering_provider_id,
                            8 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id8, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id9 AS secn_rendering_provider_id,
                            9 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id9, '')) <> ''
      UNION DISTINCT SELECT remittance_claim.claim_guid,
                            remittance_claim.payment_guid,
                            remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provdr_secndary_id10 AS secn_rendering_provider_id,
                            10 AS secn_rendering_provider_id_line_num
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod, '')) <> ''
        OR upper(coalesce(remittance_claim.rendering_provdr_secndary_id10, '')) <> '' ) AS f
   LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs.ref_secn_remittance_rendering_provider AS rrap ON upper(f.secn_rendering_provider_id) = upper(rrap.secn_rendering_provider_id)
   AND upper(f.secn_rendering_provider_id_qlfr_code) = upper(rrap.secn_rendering_provider_id_qlfr_code)) AS a 