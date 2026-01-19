CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimvoidhistory`
AS SELECT
  `ecw_factclaimvoidhistory`.claimvoidhistorykey,
  `ecw_factclaimvoidhistory`.oldclaimnumber,
  `ecw_factclaimvoidhistory`.oldclaimkey,
  `ecw_factclaimvoidhistory`.reversedclaimnumber,
  `ecw_factclaimvoidhistory`.reversedclaimkey,
  `ecw_factclaimvoidhistory`.newclaimnumber,
  `ecw_factclaimvoidhistory`.newclaimkey,
  `ecw_factclaimvoidhistory`.sourceprimarykeyvalue,
  `ecw_factclaimvoidhistory`.sourcerecordlastupdated,
  `ecw_factclaimvoidhistory`.dwlastupdatedatetime,
  `ecw_factclaimvoidhistory`.sourcesystemcode,
  `ecw_factclaimvoidhistory`.insertedby,
  `ecw_factclaimvoidhistory`.inserteddtm,
  `ecw_factclaimvoidhistory`.modifiedby,
  `ecw_factclaimvoidhistory`.modifieddtm,
  `ecw_factclaimvoidhistory`.deleteflag,
  `ecw_factclaimvoidhistory`.regionkey,
  `ecw_factclaimvoidhistory`.coid,
  `ecw_factclaimvoidhistory`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimvoidhistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimvoidhistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;