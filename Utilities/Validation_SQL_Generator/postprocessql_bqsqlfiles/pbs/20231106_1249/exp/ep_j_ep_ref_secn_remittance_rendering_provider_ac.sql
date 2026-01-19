-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_secn_remittance_rendering_provider_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_secn_remittance_rendering_provider.remittance_secn_rendering_provider_sid), CAST(0 AS BIGNUMERIC))
      FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_secn_remittance_rendering_provider) + CAST(row_number() OVER (
                                                                                                               ORDER BY upper(f.secn_rendering_provider_id_qlfr_code), upper(f.secn_rendering_provider_id)) AS BIGNUMERIC) AS remittance_secn_rendering_provider_sid, --  SID
 f.secn_rendering_provider_id_qlfr_code,
 f.secn_rendering_provider_id,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod AS secn_rendering_provider_id_qlfr_code,
             remittance_claim.rendering_provder_secndary_id1 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_1_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id1, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id2 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_2_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id2, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id3 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_3_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id3, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id4 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_4_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id4, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id5 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_5_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id5, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id6 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_6_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id6, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id7 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_7_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id7, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id8 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_8_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id8, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provder_secndary_id9 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_ref_idn_qul_9_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provder_secndary_id9, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod AS secn_rendering_provider_id_qlfr_code,
                            remittance_claim.rendering_provdr_secndary_id10 AS secn_rendering_provider_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.rndrng_prvdr_rf_idn_qul_10_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.rendering_provdr_secndary_id10, '')) NOT IN('') ) AS f) AS a 