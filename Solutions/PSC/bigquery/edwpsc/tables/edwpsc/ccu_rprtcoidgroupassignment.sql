CREATE TABLE IF NOT EXISTS edwpsc.ccu_rprtcoidgroupassignment
(
  ccucoidgroupassignmentkey INT64 NOT NULL,
  coderpracticename STRING,
  coid STRING,
  assignmentgroupvalue STRING,
  coidspecialty STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;