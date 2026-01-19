CREATE TABLE IF NOT EXISTS edwpsc.ccu_rprtvendorreference
(
  ccuvendorreferencekey INT64 NOT NULL,
  `34id` STRING,
  lastname STRING,
  firstname STRING,
  vendor STRING,
  activestatus STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  sharepointdatemodified DATETIME,
  sharepointdatecreated DATETIME,
  `group` STRING,
  area STRING,
  directors STRING,
  manager STRING,
  termdate DATETIME,
  coderstatus STRING,
  specialty STRING,
  subspecialty STRING,
  hourlybenchmark NUMERIC(33, 4),
  comments STRING,
  sharepointcreatedby STRING,
  sharepointmodifiedby STRING
)
;