-- Translation time: 2023-11-06T18:51:08.203599Z
-- Translation job ID: efa6ce37-6eb4-4610-b3f0-924f74c34d40
-- Source: eim-clin-pdoc-ccda-dev-0001/mhb_bulk_conversion_validation/20231106_1249/input/exp/j_mhb_alerting_audit_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT c.rdc_sid
   FROM
     (SELECT rdc.rdc_sid
      FROM `hca-hin-dev-cur-clinical`.edwci_staging.vwpatientalerttracker AS a
      INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_regional_data_center AS rdc ON upper(replace(trim(a.databasename), 'heartbeatDW_', '')) = upper(trim(rdc.rdc_desc))
      LEFT OUTER JOIN -- ON A.DATABASENAME = TRIM(RDC.RDC_DESC)
 `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_alert_type ON upper(coalesce(trim(a.alert_title), 'Unknown')) = upper(ref_mhb_alert_type.alert_type_desc)
      INNER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.mhb_user AS usr ON upper(a.user_name) = upper(usr.user_login_name)
      AND rdc.rdc_sid = usr.rdc_sid
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwci_base_views.ref_mhb_user_role AS url ON upper(trim(a.user_role)) = upper(trim(url.mhb_user_role_desc))
      AND url.mhb_user_role_sid = usr.mhb_user_role_sid
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf1 ON trim(cf1.facility_mnemonic_cs) = trim(a.facilitycode)
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = trim(a.patient_facility_code)
      LEFT OUTER JOIN -- LEFT JOIN EDWPF_STAGING.CLINICAL_ACCTKEYS CK
 -- ON CK.COID = CF2.COID
 -- AND CK.PAT_ACCT_NUM=(CASE WHEN OTRANSLATE(A.PATIENT_VISITNUMBER, OTRANSLATE(A.PATIENT_VISITNUMBER, '0123456789',''), '')='' THEN 0 ELSE CAST(OTRANSLATE(A.PATIENT_VISITNUMBER, OTRANSLATE(A.PATIENT_VISITNUMBER, '0123456789',''), '') AS DECIMAL(12,0)) END)
 `hca-hin-dev-cur-clinical`.edwcl_base_views.clinical_facility_location AS cfl ON trim(cfl.location_mnemonic_cs) = trim(substr(a.unitcode, strpos(a.unitcode, '_') + 1))
      WHERE usr.eff_to_date = DATE '9999-12-31'
        AND upper(usr.active_dw_ind) = 'Y' ) AS c) AS q