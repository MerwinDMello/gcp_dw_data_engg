CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtcodingvolumes`
AS SELECT
  `ecw_rprtcodingvolumes`.codingvolumekey,
  `ecw_rprtcodingvolumes`.coid,
  `ecw_rprtcodingvolumes`.claimkey,
  `ecw_rprtcodingvolumes`.claimnumber,
  `ecw_rprtcodingvolumes`.regionkey,
  `ecw_rprtcodingvolumes`.patientkey,
  `ecw_rprtcodingvolumes`.servicingproviderkey,
  `ecw_rprtcodingvolumes`.claimlinechargekey,
  `ecw_rprtcodingvolumes`.transactionbyuserkey,
  `ecw_rprtcodingvolumes`.transactiondatekey,
  `ecw_rprtcodingvolumes`.systemname,
  `ecw_rprtcodingvolumes`.praticename,
  `ecw_rprtcodingvolumes`.visitnumber,
  `ecw_rprtcodingvolumes`.rownumber,
  `ecw_rprtcodingvolumes`.recordtype,
  `ecw_rprtcodingvolumes`.snapshotdate,
  `ecw_rprtcodingvolumes`.deptcode,
  `ecw_rprtcodingvolumes`.usertype,
  `ecw_rprtcodingvolumes`.placeofservice,
  `ecw_rprtcodingvolumes`.claimstatusname,
  `ecw_rprtcodingvolumes`.visittypename
  FROM
    edwpsc_base_views.`ecw_rprtcodingvolumes`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtcodingvolumes`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;