CREATE TABLE IF NOT EXISTS edwpsc.ecw_rprtiettrendingopeniets
(
  snapshotdate DATE,
  groupname STRING,
  divisionname STRING,
  marketname STRING,
  coidname STRING,
  coidlob STRING,
  totalerrorcount INT64,
  totalagedays INT64,
  errorcountover30days INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;