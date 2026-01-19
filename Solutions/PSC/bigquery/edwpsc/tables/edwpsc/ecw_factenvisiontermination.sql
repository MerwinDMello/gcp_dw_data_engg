CREATE TABLE IF NOT EXISTS edwpsc.ecw_factenvisiontermination
(
  envisionterminationkey INT64 NOT NULL,
  npi STRING,
  lastname STRING,
  firstname STRING,
  middlename STRING,
  suffix STRING,
  degree STRING,
  sitecode STRING,
  termeffectivedate DATE,
  status STRING,
  businessdayssinceloaded INT64,
  loaddate DATE,
  worked INT64,
  workeddate DATE,
  artivaworklist INT64,
  contentworklist INT64,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;