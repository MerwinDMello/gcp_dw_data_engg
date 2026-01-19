CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountercptmeditechexpanse
(
  encountercptmtxkey INT64 NOT NULL,
  regionkey INT64,
  encounterkey INT64,
  cptcodekey INT64,
  cptcode STRING,
  cptunits INT64,
  cptorder INT64,
  cptmod1 STRING,
  visitdate DATE,
  deleteflag INT64,
  chargecode STRING,
  sourcelastupdateddate DATETIME,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  cpttransnum INT64
)
;