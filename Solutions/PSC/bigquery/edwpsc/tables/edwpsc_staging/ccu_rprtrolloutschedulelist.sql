CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_rprtrolloutschedulelist
(
  rolloutschedulelistkey INT64 NOT NULL,
  coid STRING,
  ccugolivedate DATETIME,
  ccugolivestatusvalue STRING,
  ccudiscontinueddate DATE,
  pkactivationstatusvalue STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  gme STRING
)
;