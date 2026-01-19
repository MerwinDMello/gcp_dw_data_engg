CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtcodingvolumes`
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
    edwpsc.`ecw_rprtcodingvolumes`
;