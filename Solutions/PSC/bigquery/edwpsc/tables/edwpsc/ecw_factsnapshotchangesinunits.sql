CREATE TABLE IF NOT EXISTS edwpsc.ecw_factsnapshotchangesinunits
(
  unitskey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  coid STRING,
  regionkey INT64,
  cptid INT64,
  differenceunitsvalue NUMERIC(33, 4),
  differencechargesvalue NUMERIC(33, 4),
  foundinpreviousmonth INT64,
  deletedline INT64,
  dwlastupdatedatetime DATETIME,
  claimlinechargekey INT64,
  claimkey INT64,
  cptchargesamt NUMERIC(31, 2),
  cptunits NUMERIC(31, 2)
)
PARTITION BY snapshotdate
;