CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimstatushistory`
AS SELECT
  `ecw_factclaimstatushistory`.claimstatushistorykey,
  `ecw_factclaimstatushistory`.claimkey,
  `ecw_factclaimstatushistory`.claimnumber,
  `ecw_factclaimstatushistory`.regionkey,
  `ecw_factclaimstatushistory`.coid,
  `ecw_factclaimstatushistory`.claimstatushistorymessage,
  `ecw_factclaimstatushistory`.claimstatushistoryfrom,
  `ecw_factclaimstatushistory`.claimstatushistoryto,
  `ecw_factclaimstatushistory`.claimstatushistorychangedate,
  `ecw_factclaimstatushistory`.claimstatushistorychangetime,
  `ecw_factclaimstatushistory`.claimstatushistorychangedbyuserkey,
  `ecw_factclaimstatushistory`.sourceprimarykeyvalue,
  `ecw_factclaimstatushistory`.sourcerecordlastupdated,
  `ecw_factclaimstatushistory`.dwlastupdatedatetime,
  `ecw_factclaimstatushistory`.sourcesystemcode,
  `ecw_factclaimstatushistory`.insertedby,
  `ecw_factclaimstatushistory`.inserteddtm,
  `ecw_factclaimstatushistory`.modifiedby,
  `ecw_factclaimstatushistory`.modifieddtm,
  `ecw_factclaimstatushistory`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimstatushistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimstatushistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;