CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprtcashmgmtpvbalancingreport
(
  practice STRING,
  coid STRING,
  posteddate STRING,
  pvamount NUMERIC(33, 4),
  cmtpostedamount NUMERIC(33, 4),
  variance NUMERIC(33, 4),
  pvtype STRING,
  pvtypeid INT64,
  specialistfullname STRING,
  lastnote STRING,
  notedate DATETIME,
  userid STRING,
  pvbalancingreportkey INT64 NOT NULL,
  dwlastupdatedatetime DATETIME,
  reasonnames STRING,
  selectedcoiduser STRING
)
;