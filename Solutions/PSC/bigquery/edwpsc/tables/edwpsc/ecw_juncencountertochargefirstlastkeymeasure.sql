CREATE TABLE IF NOT EXISTS edwpsc.ecw_juncencountertochargefirstlastkeymeasure
(
  encountertochargekey INT64 NOT NULL,
  lastsourcesystem STRING,
  lastsourcesystemdtm DATETIME,
  lastregionid INT64,
  laststatus STRING,
  laststatusdtm DATETIME,
  lastowner STRING,
  claimcount INT64,
  encountercptcount INT64,
  claimcptcount INT64,
  chargequantity INT64,
  pscproviderflag INT64,
  dwlastupdatedatetime DATETIME
)
;