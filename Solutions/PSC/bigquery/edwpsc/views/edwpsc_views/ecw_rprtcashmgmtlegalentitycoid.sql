CREATE OR REPLACE VIEW edwpsc_views.`ecw_rprtcashmgmtlegalentitycoid`
AS SELECT
  `ecw_rprtcashmgmtlegalentitycoid`.legalentityname,
  `ecw_rprtcashmgmtlegalentitycoid`.taxid,
  `ecw_rprtcashmgmtlegalentitycoid`.legalentityenddate,
  `ecw_rprtcashmgmtlegalentitycoid`.arsystem,
  `ecw_rprtcashmgmtlegalentitycoid`.islegalentitythirdparty,
  `ecw_rprtcashmgmtlegalentitycoid`.legalentitythirdpartytype,
  `ecw_rprtcashmgmtlegalentitycoid`.specialistlastname,
  `ecw_rprtcashmgmtlegalentitycoid`.specialistfirstname,
  `ecw_rprtcashmgmtlegalentitycoid`.legalentitymappedcoid,
  `ecw_rprtcashmgmtlegalentitycoid`.coid,
  `ecw_rprtcashmgmtlegalentitycoid`.bankname,
  `ecw_rprtcashmgmtlegalentitycoid`.bankaccountnumber,
  `ecw_rprtcashmgmtlegalentitycoid`.lockboxnumber,
  `ecw_rprtcashmgmtlegalentitycoid`.treasuryvalueunit,
  `ecw_rprtcashmgmtlegalentitycoid`.selectedcoiduser,
  `ecw_rprtcashmgmtlegalentitycoid`.isopenconnect,
  `ecw_rprtcashmgmtlegalentitycoid`.legalentitycoidenddate,
  `ecw_rprtcashmgmtlegalentitycoid`.bankaccountenddate,
  `ecw_rprtcashmgmtlegalentitycoid`.bankenddate
  FROM
    edwpsc_base_views.`ecw_rprtcashmgmtlegalentitycoid`
  INNER JOIN edwpsc_base_views.secref_facility
  ON LPAD(TRIM(secref_facility.co_id, ' '), 5, '0') = LPAD(TRIM(`ecw_rprtcashmgmtlegalentitycoid`.coid, ' '), 5, '0')
  AND TRIM(secref_facility.user_id, ' ') = TRIM(session_user(), ' ')
;