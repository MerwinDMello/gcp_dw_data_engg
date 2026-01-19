CREATE OR REPLACE VIEW edwpsc_views.`pv_factrh1201`
AS SELECT
  `pv_factrh1201`.rh1201key,
  `pv_factrh1201`.importdatekey,
  `pv_factrh1201`.claimkey,
  `pv_factrh1201`.claimnumber,
  `pv_factrh1201`.regionkey,
  `pv_factrh1201`.coid,
  `pv_factrh1201`.rh1201claimid,
  `pv_factrh1201`.rh1201insurancebilledname,
  `pv_factrh1201`.rh1201billstatuscode,
  `pv_factrh1201`.rh1201billclaimstatuskey,
  `pv_factrh1201`.rh1201releasestatuskey,
  `pv_factrh1201`.rh1201typeofbill,
  `pv_factrh1201`.rh1201claimdatekey,
  `pv_factrh1201`.rh1201stmtthrudatekey,
  `pv_factrh1201`.rh1201totalamt,
  `pv_factrh1201`.rh1201userid,
  `pv_factrh1201`.rh1201holdcode,
  `pv_factrh1201`.rh1201holdcodeprefixkey,
  `pv_factrh1201`.rh1201filename,
  `pv_factrh1201`.sourceprimarykeyvalue,
  `pv_factrh1201`.dwlastupdatedatetime,
  `pv_factrh1201`.sourcesystemcode,
  `pv_factrh1201`.insertedby,
  `pv_factrh1201`.inserteddtm,
  `pv_factrh1201`.modifiedby,
  `pv_factrh1201`.modifieddtm
  FROM
    edwpsc_base_views.`pv_factrh1201`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`pv_factrh1201`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;