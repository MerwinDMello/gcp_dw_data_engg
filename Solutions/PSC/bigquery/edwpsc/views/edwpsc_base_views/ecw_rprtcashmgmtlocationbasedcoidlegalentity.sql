CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtcashmgmtlocationbasedcoidlegalentity`
AS SELECT
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.legalentityname,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.taxid,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.legalentityenddate,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.arsystem,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.islegalentitythirdparty,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.legalentitythirdpartytype,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.specialistlastname,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.specialistfirstname,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.locationmappedcoid,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.locationname,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.bankname,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.bankaccountnumber,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.lockboxnumber,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.sitedepositaccount,
  `ecw_rprtcashmgmtlocationbasedcoidlegalentity`.treasuryvalueunit
  FROM
    edwpsc.`ecw_rprtcashmgmtlocationbasedcoidlegalentity`
;