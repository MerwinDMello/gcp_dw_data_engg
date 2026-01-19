CREATE TABLE IF NOT EXISTS edwpsc.pv_factsnapshotchangesinunits
(
  unitskey INT64 NOT NULL,
  monthid INT64,
  snapshotdate DATE,
  coid STRING,
  regionkey INT64,
  practiceid STRING,
  cptid STRING,
  differenceunitsvalue NUMERIC(33, 4),
  differencechargesvalue NUMERIC(33, 4),
  foundinpreviousmonth INT64,
  deletedline INT64,
  dwlastupdatedatetime DATETIME
)
PARTITION BY snapshotdate
;