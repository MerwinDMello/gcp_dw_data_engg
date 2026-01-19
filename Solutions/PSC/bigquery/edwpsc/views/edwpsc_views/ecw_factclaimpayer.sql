CREATE OR REPLACE VIEW edwpsc_views.`ecw_factclaimpayer`
AS SELECT
  `ecw_factclaimpayer`.claimpayerkey,
  `ecw_factclaimpayer`.claimkey,
  `ecw_factclaimpayer`.claimnumber,
  `ecw_factclaimpayer`.coid,
  `ecw_factclaimpayer`.seqnumber,
  `ecw_factclaimpayer`.payeriplankey,
  `ecw_factclaimpayer`.payergroupnumber,
  `ecw_factclaimpayer`.payergroupname,
  `ecw_factclaimpayer`.payersubscribernumber,
  `ecw_factclaimpayer`.payerclaimindicator,
  `ecw_factclaimpayer`.payerliabilityowner,
  `ecw_factclaimpayer`.payersourcechangedflag,
  `ecw_factclaimpayer`.payersourceprimarykeyvalue,
  `ecw_factclaimpayer`.payersourcetablelastupdated,
  `ecw_factclaimpayer`.dwlastupdatedatetime,
  `ecw_factclaimpayer`.sourcesystemcode,
  `ecw_factclaimpayer`.insertedby,
  `ecw_factclaimpayer`.inserteddtm,
  `ecw_factclaimpayer`.modifiedby,
  `ecw_factclaimpayer`.modifieddtm,
  `ecw_factclaimpayer`.deleteflag,
  `ecw_factclaimpayer`.priorauthno,
  `ecw_factclaimpayer`.regionkey,
  `ecw_factclaimpayer`.arclaimflag,
  `ecw_factclaimpayer`.archivedrecord
  FROM
    edwpsc_base_views.`ecw_factclaimpayer`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_factclaimpayer`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;