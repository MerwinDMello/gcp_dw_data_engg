CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpsfinclasscross
(
  psfckey NUMERIC(29),
  psfciplangroup STRING,
  psfcrossfincls STRING,
  psfcsource STRING,
  psfcsourcefincls STRING,
  psfcsourcetype STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;