CREATE OR REPLACE VIEW edwpsc_views.`epic_factnotehistory`
AS SELECT
  `epic_factnotehistory`.notehistorykey,
  `epic_factnotehistory`.regionkey,
  `epic_factnotehistory`.coid,
  `epic_factnotehistory`.accountkey,
  `epic_factnotehistory`.accountid,
  `epic_factnotehistory`.patientkey,
  `epic_factnotehistory`.patientid,
  `epic_factnotehistory`.claimkey,
  `epic_factnotehistory`.invoicenumber,
  `epic_factnotehistory`.encounterkey,
  `epic_factnotehistory`.encounterid,
  `epic_factnotehistory`.notetype,
  `epic_factnotehistory`.notesummary,
  `epic_factnotehistory`.note,
  `epic_factnotehistory`.notestatus,
  `epic_factnotehistory`.notecreateddate,
  `epic_factnotehistory`.notecreatedtime,
  `epic_factnotehistory`.priorityflag,
  `epic_factnotehistory`.notecreatedbyuserkey,
  `epic_factnotehistory`.notecreatedbyuserid,
  `epic_factnotehistory`.notesource,
  `epic_factnotehistory`.sourceaprimarykeyvalue,
  `epic_factnotehistory`.dwlastupdatedatetime,
  `epic_factnotehistory`.sourcesystemcode,
  `epic_factnotehistory`.insertedby,
  `epic_factnotehistory`.inserteddtm,
  `epic_factnotehistory`.modifiedby,
  `epic_factnotehistory`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factnotehistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factnotehistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;