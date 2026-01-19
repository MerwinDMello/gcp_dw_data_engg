CREATE OR REPLACE VIEW edwpsc_views.`openconnect_factpscmtxrecon`
AS SELECT
  `openconnect_factpscmtxrecon`.openconnectpscmtxreconkey,
  `openconnect_factpscmtxrecon`.messagecontrolid,
  `openconnect_factpscmtxrecon`.batchdate,
  `openconnect_factpscmtxrecon`.visitnumber,
  `openconnect_factpscmtxrecon`.patientaccountnumber,
  `openconnect_factpscmtxrecon`.procedurecode,
  `openconnect_factpscmtxrecon`.cptcode,
  `openconnect_factpscmtxrecon`.servicedate,
  `openconnect_factpscmtxrecon`.cptunits,
  `openconnect_factpscmtxrecon`.facilitylocationid,
  `openconnect_factpscmtxrecon`.coid,
  `openconnect_factpscmtxrecon`.filedate,
  `openconnect_factpscmtxrecon`.filename,
  `openconnect_factpscmtxrecon`.fileimporteddate,
  `openconnect_factpscmtxrecon`.sourcefiletype,
  `openconnect_factpscmtxrecon`.sendingmtx,
  `openconnect_factpscmtxrecon`.insertedby,
  `openconnect_factpscmtxrecon`.inserteddtm,
  `openconnect_factpscmtxrecon`.modifiedby,
  `openconnect_factpscmtxrecon`.modifieddtm,
  `openconnect_factpscmtxrecon`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`openconnect_factpscmtxrecon`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`openconnect_factpscmtxrecon`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;