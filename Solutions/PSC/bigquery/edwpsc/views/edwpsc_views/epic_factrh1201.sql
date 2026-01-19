CREATE OR REPLACE VIEW edwpsc_views.`epic_factrh1201`
AS SELECT
  `epic_factrh1201`.rh1201key,
  `epic_factrh1201`.importdatekey,
  `epic_factrh1201`.claimkey,
  `epic_factrh1201`.claimnumber,
  `epic_factrh1201`.regionkey,
  `epic_factrh1201`.coid,
  `epic_factrh1201`.rh1201claimid,
  `epic_factrh1201`.rh1201insurancebilledname,
  `epic_factrh1201`.rh1201billstatuscode,
  `epic_factrh1201`.rh1201billclaimstatuskey,
  `epic_factrh1201`.rh1201releasestatuskey,
  `epic_factrh1201`.rh1201typeofbill,
  `epic_factrh1201`.rh1201claimdatekey,
  `epic_factrh1201`.rh1201stmtthrudatekey,
  `epic_factrh1201`.rh1201totalamt,
  `epic_factrh1201`.rh1201userid,
  `epic_factrh1201`.rh1201holdcode,
  `epic_factrh1201`.rh1201holdcodeprefixkey,
  `epic_factrh1201`.rh1201filename,
  `epic_factrh1201`.sourceprimarykeyvalue,
  `epic_factrh1201`.dwlastupdatedatetime,
  `epic_factrh1201`.sourcesystemcode,
  `epic_factrh1201`.insertedby,
  `epic_factrh1201`.inserteddtm,
  `epic_factrh1201`.modifiedby,
  `epic_factrh1201`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factrh1201`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`epic_factrh1201`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;