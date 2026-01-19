CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsccupicklistmaint
(
  psccupklstkey NUMERIC(29),
  psccupklstactive STRING,
  psccupklstfieldnm STRING,
  psccupklstfldvalue STRING,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  psccupklstactdte DATE,
  psccupklstinactdate DATE,
  psccupklstlastupdusr STRING,
  psccupklstupddte DATE
)
;