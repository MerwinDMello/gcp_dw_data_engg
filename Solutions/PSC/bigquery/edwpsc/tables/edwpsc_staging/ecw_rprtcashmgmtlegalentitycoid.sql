CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprtcashmgmtlegalentitycoid
(
  legalentityname STRING,
  taxid INT64,
  legalentityenddate DATETIME,
  arsystem STRING,
  islegalentitythirdparty BOOL,
  legalentitythirdpartytype STRING,
  specialistlastname STRING,
  specialistfirstname STRING,
  legalentitymappedcoid STRING,
  coid STRING,
  bankname STRING,
  bankaccountnumber STRING,
  lockboxnumber INT64,
  treasuryvalueunit STRING,
  selectedcoiduser STRING,
  isopenconnect BOOL,
  legalentitycoidenddate DATETIME,
  bankaccountenddate DATETIME,
  bankenddate DATETIME
)
;