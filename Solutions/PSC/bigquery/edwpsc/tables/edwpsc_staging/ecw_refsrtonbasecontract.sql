CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refsrtonbasecontract
(
  onbasecontractkey INT64 NOT NULL,
  onbasecontractlocation STRING,
  onbasecontractsourcesystem STRING,
  onbasecontractlobtype STRING,
  onbasecontractqueuename STRING,
  onbasecontractcontrolnum STRING,
  onbasecontractexecutivename STRING,
  onbasecontractdirectorname STRING,
  onbasecontractmanagername STRING,
  onbasecontractresponsibledepartment STRING,
  onbasecontractvendorflag STRING,
  onbasecontractvendorname STRING,
  onbasecontractemployeetype STRING,
  onbasecontractlob STRING,
  onbasecontractactiveflag STRING,
  onbasecontracteffectivedatekey DATE,
  onbasecontracttermeddatekey DATE,
  sourceaprimarykeyvalue INT64,
  deleteflag INT64 NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME
)
;