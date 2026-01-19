-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_care_attachment_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT rdc.rdc_sid,
          max(mu.user_login_name) AS user_login_name,
          ck.patient_dw_id,
          CAST(trim(attachment.attach_datetime) AS DATETIME) AS attach_date_time,
          max(coalesce(mu2.user_login_name, 'Unknown')) AS creator_login_name,
          coalesce(mur2.mhb_user_role_sid, -1) AS creator_mhb_user_role_sid,
          rmu2.mhb_unit_id AS created_mhb_unit_id,
          max(coalesce(cfl2.location_mnemonic_cs, 'Unknown')) AS created_location_mnemonic_cs,
          max(coalesce(cfl2.company_code, 'H')) AS creator_company_code,
          max(coalesce(cfl2.coid, '99999')) AS creator_coid,
          max(coalesce(cf2.company_code, 'H')) AS patient_company_code,
          max(coalesce(cf2.coid, '99999')) AS patient_coid,
          ck.pat_acct_num,
          coalesce(rmur.mhb_user_role_sid, -1) AS mhb_user_role_sid,
          rmu1.mhb_unit_id AS mhb_user_signin_unit_id,
          max(coalesce(cfl1.location_mnemonic_cs, 'Unknown')) AS signin_location_mnemonic_cs,
          max(coalesce(cfl1.company_code, 'H')) AS company_code,
          max(coalesce(cfl1.coid, '99999')) AS coid,
          attachment.trail_id AS mhb_audit_trail_num,
          max(attachment.attach_source) AS detach_source_text,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-clinical`.edwci_staging.vwdynamiccareattachment AS attachment
   INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(substr(trim(attachment.databasename), strpos(trim(attachment.databasename), '_') + 1)) = upper(rdc.rdc_desc)
   INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS mu ON upper(attachment.owner_user_name) = upper(mu.user_login_name)
   AND rdc.rdc_sid = mu.rdc_sid
   INNER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = trim(attachment.patient_facilitycode)
   INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
   AND ck.pat_acct_num = ROUND(CASE translate(attachment.patient_visitnumber, translate(attachment.patient_visitnumber, '0123456789', ''), '')
                                   WHEN '' THEN NUMERIC '0'
                                   ELSE CAST(translate(attachment.patient_visitnumber, translate(attachment.patient_visitnumber, '0123456789', ''), '') AS NUMERIC)
                               END, 0, 'ROUND_HALF_EVEN')
   INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu2 ON rmu2.rdc_sid = rdc.rdc_sid
   AND attachment.created_unit_id = rmu2.mhb_unit_id
   INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_unit AS rmu1 ON rmu1.rdc_sid = rdc.rdc_sid
   AND attachment.created_unit_id = rmu1.mhb_unit_id
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS rmur ON upper(trim(attachment.owner_user_role)) = upper(trim(rmur.mhb_user_role_desc))
   AND rmur.mhb_user_role_sid = mu.mhb_user_role_sid
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS mu2 ON upper(attachment.created_user_name) = upper(mu2.user_login_name)
   AND rdc.rdc_sid = mu2.rdc_sid
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS mur2 ON upper(trim(attachment.created_user_role)) = upper(trim(mur2.mhb_user_role_desc))
   AND mur2.mhb_user_role_sid = mu2.mhb_user_role_sid
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility_location AS cfl2 ON trim(cfl2.location_mnemonic_cs) = trim(substr(attachment.created_unitcode, strpos(attachment.created_unitcode, '_') + 1))
   AND trim(cfl2.facility_mnemonic_cs) = trim(attachment.created_facilitycode)
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility_location AS cfl1 ON trim(cfl1.location_mnemonic_cs) = trim(substr(attachment.owner_unitcode, strpos(attachment.owner_unitcode, '_') + 1))
   AND trim(cfl1.facility_mnemonic_cs) = trim(attachment.owner_facilitycode)
   WHERE CAST(trim(attachment.attach_datetime) AS DATETIME) BETWEEN CAST(mu.eff_from_date AS DATETIME) AND CAST(mu.eff_to_date AS DATETIME)
     AND CAST(trim(attachment.attach_datetime) AS DATETIME) BETWEEN CAST(mu2.eff_from_date AS DATETIME) AND CAST(mu2.eff_to_date AS DATETIME)
   GROUP BY 1,
            upper(mu.user_login_name),
            3,
            4,
            upper(coalesce(mu2.user_login_name, 'Unknown')),
            6,
            7,
            upper(coalesce(cfl2.location_mnemonic_cs, 'Unknown')),
            upper(coalesce(cfl2.company_code, 'H')),
            upper(coalesce(cfl2.coid, '99999')),
            upper(coalesce(cf2.company_code, 'H')),
            upper(coalesce(cf2.coid, '99999')),
            13,
            14,
            15,
            upper(coalesce(cfl1.location_mnemonic_cs, 'Unknown')),
            upper(coalesce(cfl1.company_code, 'H')),
            upper(coalesce(cfl1.coid, '99999')),
            19,
            upper(attachment.attach_source),
            21,
            22) AS q