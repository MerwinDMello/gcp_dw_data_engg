CREATE OR REPLACE VIEW edwpsc_views.`pv_factrhclaimhistory`
AS SELECT
  `pv_factrhclaimhistory`.rhclaimhistorykey,
  `pv_factrhclaimhistory`.claimkey,
  `pv_factrhclaimhistory`.claimnumber,
  `pv_factrhclaimhistory`.coid,
  `pv_factrhclaimhistory`.regionkey,
  `pv_factrhclaimhistory`.importdatekey,
  `pv_factrhclaimhistory`.rhclaimhistoryclientid,
  `pv_factrhclaimhistory`.rhclaimhistorysubmissiondatekey,
  `pv_factrhclaimhistory`.rhclaimhistorybridgefilenumber,
  `pv_factrhclaimhistory`.rhclaimhistoryclaimid,
  `pv_factrhclaimhistory`.rhclaimhistorypatientcontrolnbr,
  `pv_factrhclaimhistory`.rhclaimhistoryoriginalclaimstatuscode,
  `pv_factrhclaimhistory`.rhclaimhistorypayerindicator,
  `pv_factrhclaimhistory`.rhclaimhistoryoriginalclaimamount,
  `pv_factrhclaimhistory`.rhclaimhistoryoriginalerrorcount,
  `pv_factrhclaimhistory`.rhclaimhistoryreleaseddatekey,
  `pv_factrhclaimhistory`.rhclaimhistoryclaimstate,
  `pv_factrhclaimhistory`.rhclaimhistoryholdcode,
  `pv_factrhclaimhistory`.rhclaimhistorycurrentclaimstatuscode,
  `pv_factrhclaimhistory`.rhclaimhistoryerrorcount,
  `pv_factrhclaimhistory`.rhclaimhistorytotalclaimamount,
  `pv_factrhclaimhistory`.rhclaimhistoryfilename,
  `pv_factrhclaimhistory`.sourceprimarykeyvalue,
  `pv_factrhclaimhistory`.dwlastupdatedatetime,
  `pv_factrhclaimhistory`.sourcesystemcode,
  `pv_factrhclaimhistory`.insertedby,
  `pv_factrhclaimhistory`.inserteddtm,
  `pv_factrhclaimhistory`.modifiedby,
  `pv_factrhclaimhistory`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factrhclaimhistory`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factrhclaimhistory`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;