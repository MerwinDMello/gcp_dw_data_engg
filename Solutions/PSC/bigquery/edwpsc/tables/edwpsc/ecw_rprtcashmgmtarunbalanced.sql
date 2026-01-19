CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprtcashmgmtarunbalanced
(
  coid INT64,
  posteddate STRING,
  arsystemname STRING,
  arsystemtypeid INT64,
  arpostedamount NUMERIC(33, 4),
  cmtpostedamount NUMERIC(33, 4),
  siteid STRING,
  lastnote STRING,
  notedate DATETIME,
  userid STRING,
  fullname STRING,
  arunbalancedkey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  reasons STRING,
  selectedcoiduser STRING
)
;