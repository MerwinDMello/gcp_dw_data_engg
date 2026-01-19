CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsccuproviderrules
(
  psccuprrulekey NUMERIC(29),
  psccuprrulconfeffdte DATE,
  psccuprrulconfigkey STRING,
  psccuprrulconfiglbl STRING,
  psccuprrulconflkflg STRING,
  psccuprrulcontrmdte DATE,
  psccuprruleid NUMERIC(29),
  psccuprrulinvowner STRING,
  psccuprrulrequestor STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;