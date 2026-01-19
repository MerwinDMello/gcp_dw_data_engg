CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountermedaxioncodingcptcodes
(
  medaxioncodingcptcodeskey INT64 NOT NULL,
  medaxioncodingstatuskey INT64,
  cptcode STRING,
  modifiercodes STRING,
  diagnosiscodes STRING,
  cptprovidername STRING,
  cptcount INT64,
  deleteflag INT64,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;