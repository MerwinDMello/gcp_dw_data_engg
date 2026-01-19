CREATE TABLE IF NOT EXISTS edwpsc.ccu_refepicworkqueues
(
  workqueuekey INT64 NOT NULL,
  workqueueid STRING,
  ccuqueueflag INT64,
  practicequeueflag INT64,
  accountresolutionqueueflag INT64,
  vendorqueueflag INT64,
  ccuvendorqueueflag INT64,
  active INT64,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  regionkey INT64
)
;