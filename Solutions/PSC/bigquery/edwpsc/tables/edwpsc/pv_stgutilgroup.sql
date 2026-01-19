CREATE TABLE IF NOT EXISTS edwpsc.pv_stgutilgroup
(
  group_name STRING,
  entity STRING,
  key_name STRING,
  subkey STRING,
  active STRING,
  text STRING,
  number NUMERIC(33, 4),
  description STRING,
  utilgrouppk STRING,
  isupdateable BOOL,
  inserteddtm DATETIME,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcephysicaldeleteflag INT64
)
;