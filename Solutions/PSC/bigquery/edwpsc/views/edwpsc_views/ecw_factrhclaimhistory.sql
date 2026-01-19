CREATE OR REPLACE VIEW edwpsc_views.`ecw_factrhclaimhistory`
AS SELECT
  `ecw_factrhclaimhistory`.rhclaimhistorykey,
  `ecw_factrhclaimhistory`.claimkey,
  `ecw_factrhclaimhistory`.claimnumber,
  `ecw_factrhclaimhistory`.coid,
  `ecw_factrhclaimhistory`.importdatekey,
  `ecw_factrhclaimhistory`.rhclaimhistoryclientid,
  `ecw_factrhclaimhistory`.rhclaimhistorysubmissiondatekey,
  `ecw_factrhclaimhistory`.rhclaimhistorybridgefilenumber,
  `ecw_factrhclaimhistory`.rhclaimhistoryclaimid,
  `ecw_factrhclaimhistory`.rhclaimhistorypatientcontrolnbr,
  `ecw_factrhclaimhistory`.rhclaimhistoryoriginalclaimstatuscode,
  `ecw_factrhclaimhistory`.rhclaimhistorypayerindicator,
  `ecw_factrhclaimhistory`.rhclaimhistoryoriginalclaimamount,
  `ecw_factrhclaimhistory`.rhclaimhistoryoriginalerrorcount,
  `ecw_factrhclaimhistory`.rhclaimhistoryreleaseddatekey,
  `ecw_factrhclaimhistory`.rhclaimhistoryclaimstate,
  `ecw_factrhclaimhistory`.rhclaimhistoryholdcode,
  `ecw_factrhclaimhistory`.rhclaimhistorycurrentclaimstatuscode,
  `ecw_factrhclaimhistory`.rhclaimhistoryerrorcount,
  `ecw_factrhclaimhistory`.rhclaimhistorytotalclaimamount,
  `ecw_factrhclaimhistory`.sourceprimarykeyvalue,
  `ecw_factrhclaimhistory`.dwlastupdatedatetime,
  `ecw_factrhclaimhistory`.sourcesystemcode,
  `ecw_factrhclaimhistory`.insertedby,
  `ecw_factrhclaimhistory`.inserteddtm,
  `ecw_factrhclaimhistory`.modifiedby,
  `ecw_factrhclaimhistory`.modifieddtm,
  `ecw_factrhclaimhistory`.billedtoid,
  `ecw_factrhclaimhistory`.fullclaimnumber,
  `ecw_factrhclaimhistory`.regionkey
  FROM
    edwpsc_base_views.`ecw_factrhclaimhistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factrhclaimhistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;