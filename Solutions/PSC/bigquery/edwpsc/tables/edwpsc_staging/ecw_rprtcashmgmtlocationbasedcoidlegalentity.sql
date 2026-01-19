CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_rprtcashmgmtlocationbasedcoidlegalentity
(
  legalentityname STRING,
  taxid INT64,
  legalentityenddate DATETIME,
  arsystem STRING,
  islegalentitythirdparty BOOL,
  legalentitythirdpartytype STRING,
  specialistlastname STRING,
  specialistfirstname STRING,
  locationmappedcoid STRING,
  locationname STRING,
  bankname STRING,
  bankaccountnumber STRING,
  lockboxnumber INT64,
  sitedepositaccount BOOL,
  treasuryvalueunit STRING
)
;