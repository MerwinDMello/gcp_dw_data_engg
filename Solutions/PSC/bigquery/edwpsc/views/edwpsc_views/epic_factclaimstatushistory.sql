CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimstatushistory`
AS SELECT
  `epic_factclaimstatushistory`.claimstatushistorykey,
  `epic_factclaimstatushistory`.claimkey,
  `epic_factclaimstatushistory`.claimnumber,
  `epic_factclaimstatushistory`.visitnumber,
  `epic_factclaimstatushistory`.regionkey,
  `epic_factclaimstatushistory`.coid,
  `epic_factclaimstatushistory`.invoicenumber,
  `epic_factclaimstatushistory`.claimstatushistorymessage,
  `epic_factclaimstatushistory`.claimstatushistoryfrom,
  `epic_factclaimstatushistory`.claimstatushistoryto,
  `epic_factclaimstatushistory`.claimstatushistorychangedate,
  `epic_factclaimstatushistory`.claimstatushistorychangetime,
  `epic_factclaimstatushistory`.claimstatustokey,
  `epic_factclaimstatushistory`.claimstatushistorychangedbyuserkey,
  `epic_factclaimstatushistory`.claimerrormessage,
  `epic_factclaimstatushistory`.claimstatusnote,
  `epic_factclaimstatushistory`.sourceaprimarykeyvalue,
  `epic_factclaimstatushistory`.sourcearecordlastupdated,
  `epic_factclaimstatushistory`.sourcebprimarykeyvalue,
  `epic_factclaimstatushistory`.sourcebrecordlastupdated,
  `epic_factclaimstatushistory`.dwlastupdatedatetime,
  `epic_factclaimstatushistory`.sourcesystemcode,
  `epic_factclaimstatushistory`.insertedby,
  `epic_factclaimstatushistory`.inserteddtm,
  `epic_factclaimstatushistory`.modifiedby,
  `epic_factclaimstatushistory`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimstatushistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimstatushistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;