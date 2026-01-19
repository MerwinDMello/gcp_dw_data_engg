CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_factclaimvoidhistory`
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
    edwpsc.`ecw_factclaimvoidhistory`
;