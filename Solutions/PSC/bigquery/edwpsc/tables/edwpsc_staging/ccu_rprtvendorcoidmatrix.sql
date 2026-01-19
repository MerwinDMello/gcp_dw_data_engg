CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtvendorcoidmatrix
(
  vendorcoidmatrixkey INT64 NOT NULL,
  coid STRING,
  coidname STRING,
  active BOOL,
  systemvalue STRING,
  vendorvalue STRING,
  assigneddate DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;