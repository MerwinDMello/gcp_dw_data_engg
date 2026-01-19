CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimnotehistory`
AS SELECT
  `ecw_factclaimnotehistory`.claimstatusnotekey,
  `ecw_factclaimnotehistory`.claimkey,
  `ecw_factclaimnotehistory`.claimnumber,
  `ecw_factclaimnotehistory`.regionkey,
  `ecw_factclaimnotehistory`.coid,
  `ecw_factclaimnotehistory`.claimnote,
  `ecw_factclaimnotehistory`.claimnotecreateddate,
  `ecw_factclaimnotehistory`.claimnotecreatedtime,
  `ecw_factclaimnotehistory`.claimnotecreatedbyuserkey,
  `ecw_factclaimnotehistory`.sourceprimarykeyvalue,
  `ecw_factclaimnotehistory`.sourcerecordlastupdated,
  `ecw_factclaimnotehistory`.dwlastupdatedatetime,
  `ecw_factclaimnotehistory`.sourcesystemcode,
  `ecw_factclaimnotehistory`.insertedby,
  `ecw_factclaimnotehistory`.inserteddtm,
  `ecw_factclaimnotehistory`.modifiedby,
  `ecw_factclaimnotehistory`.modifieddtm,
  `ecw_factclaimnotehistory`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimnotehistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimnotehistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;