CREATE OR REPLACE VIEW edwpsc_views.`pv_factnotehistory`
AS SELECT
  `pv_factnotehistory`.notehistorykey,
  `pv_factnotehistory`.regionkey,
  `pv_factnotehistory`.practicekey,
  `pv_factnotehistory`.coid,
  `pv_factnotehistory`.patientkey,
  `pv_factnotehistory`.note,
  `pv_factnotehistory`.notetype,
  `pv_factnotehistory`.notecreateddate,
  `pv_factnotehistory`.notecreatedtime,
  `pv_factnotehistory`.notecreatedbyuserkey,
  `pv_factnotehistory`.notecreatedbyuserid,
  `pv_factnotehistory`.notelastupdatedbyuserkey,
  `pv_factnotehistory`.notelastupdatedbyuserid,
  `pv_factnotehistory`.sourceprimarykeyvalue,
  `pv_factnotehistory`.notespk,
  `pv_factnotehistory`.patientsourceguid,
  `pv_factnotehistory`.activeflag,
  `pv_factnotehistory`.sourcerecordlastupdated,
  `pv_factnotehistory`.dwlastupdatedatetime,
  `pv_factnotehistory`.sourcesystemcode,
  `pv_factnotehistory`.insertedby,
  `pv_factnotehistory`.inserteddtm,
  `pv_factnotehistory`.modifiedby,
  `pv_factnotehistory`.modifieddtm,
  `pv_factnotehistory`.claimkey,
  `pv_factnotehistory`.claimnumber,
  `pv_factnotehistory`.codernoteflag
  FROM
    edwpsc_base_views.`pv_factnotehistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factnotehistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;