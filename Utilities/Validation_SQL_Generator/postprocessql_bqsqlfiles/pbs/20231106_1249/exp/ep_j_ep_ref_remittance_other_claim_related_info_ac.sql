-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ep_j_ep_ref_remittance_other_claim_related_info_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_other_claim_related_info.ref_sid), CAST(0 AS BIGNUMERIC))
      FROM `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_other_claim_related_info) + CAST(row_number() OVER (
                                                                                                                ORDER BY upper(f.reference_id_qualifier_code), upper(f.reference_id)) AS BIGNUMERIC) AS ref_sid, --  SID
 f.reference_id_qualifier_code,
 f.reference_id,
 'E' AS source_system_code,
 timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_claim.othr_clm_rel_ref_idn_qual1_cod AS reference_id_qualifier_code,
             remittance_claim.other_claim_related_id1 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual1_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id1, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual2_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id2 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual2_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id2, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual3_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id3 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual3_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id3, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual4_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id4 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual4_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id4, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual5_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id5 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual5_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id5, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual6_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id6 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual6_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id6, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual7_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id7 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual7_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id7, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual8_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id8 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual8_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id8, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qual9_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id9 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qual9_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id9, '')) NOT IN('')
      UNION DISTINCT SELECT remittance_claim.othr_clm_rel_ref_idn_qul10_cod AS reference_id_qualifier_code,
                            remittance_claim.other_claim_related_id10 AS reference_id
      FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_claim
      WHERE upper(coalesce(remittance_claim.othr_clm_rel_ref_idn_qul10_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.other_claim_related_id10, '')) NOT IN('') ) AS f) AS a 