CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimpayer`
AS SELECT
  `epic_factclaimpayer`.claimpayerkey,
  `epic_factclaimpayer`.claimkey,
  `epic_factclaimpayer`.claimnumber,
  `epic_factclaimpayer`.visitnumber,
  `epic_factclaimpayer`.regionkey,
  `epic_factclaimpayer`.coid,
  `epic_factclaimpayer`.seqnumber,
  `epic_factclaimpayer`.payeriplankey,
  `epic_factclaimpayer`.payergroupnumber,
  `epic_factclaimpayer`.payergroupname,
  `epic_factclaimpayer`.payersubscribernumber,
  `epic_factclaimpayer`.payerclaimindicator,
  `epic_factclaimpayer`.payerliabilityowner,
  `epic_factclaimpayer`.priorauthno,
  `epic_factclaimpayer`.deleteflag,
  `epic_factclaimpayer`.payersourcechangedflag,
  `epic_factclaimpayer`.sourceaprimarykeyvalue,
  `epic_factclaimpayer`.sourceatablelastupdated,
  `epic_factclaimpayer`.sourcebprimarykeyvalue,
  `epic_factclaimpayer`.sourcebtablelastupdated,
  `epic_factclaimpayer`.dwlastupdatedatetime,
  `epic_factclaimpayer`.sourcesystemcode,
  `epic_factclaimpayer`.insertedby,
  `epic_factclaimpayer`.inserteddtm,
  `epic_factclaimpayer`.modifiedby,
  `epic_factclaimpayer`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimpayer`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factclaimpayer`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;