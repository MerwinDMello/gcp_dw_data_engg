CREATE OR REPLACE VIEW edwpsc_base_views.`openconnect_factpscmtxrecon`
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
    edwpsc.`openconnect_factpscmtxrecon`
;