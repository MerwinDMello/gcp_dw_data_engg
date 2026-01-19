CREATE TABLE IF NOT EXISTS edwpsc_staging.ccu_refnoncustomernpiconfig_history
(
  ccunoncustomernpiconfigkey INT64 NOT NULL,
  ccunoncustomerconfigkey INT64,
  providernpi STRING NOT NULL,
  executionorder INT64,
  effectivedatekey DATE,
  terminationdatekey DATE,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING,
  modifieddtm DATETIME NOT NULL,
  approvedflag STRING,
  approveddate DATE,
  approvedbyid STRING,
  sysstarttime DATETIME NOT NULL,
  sysendtime DATETIME NOT NULL,
  coid STRING NOT NULL
)
;