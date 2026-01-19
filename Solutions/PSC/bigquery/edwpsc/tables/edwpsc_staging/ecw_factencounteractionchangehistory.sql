CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencounteractionchangehistory
(
  id INT64,
  encid INT64 NOT NULL,
  logdetails STRING,
  actiontype STRING,
  userid INT64,
  modifydate DATETIME,
  modifiedcolumns STRING,
  username STRING,
  regionkey INT64,
  loadkey INT64,
  hashnomatch INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  lastchangedby INT64,
  dwlastupdatedatetime DATETIME
)
;