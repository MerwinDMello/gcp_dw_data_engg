CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factqgendaproviderhours
(
  qgendaproviderhourskey INT64 NOT NULL,
  providerschedulesk NUMERIC(29),
  scheduledcoid STRING,
  companycode STRING,
  providershifttypedesc STRING,
  providerstaffcategorydesc STRING,
  providerlastname STRING,
  providerfirstname STRING,
  providerpersondwid NUMERIC(29),
  providernpi STRING,
  providerstafftypedesc STRING,
  scheduledstartdatetime DATETIME,
  scheduledenddatetime DATETIME,
  providercreditedhourqty NUMERIC(33, 4),
  deleteflag INT64,
  providerscheduledtypedesc STRING,
  sourcelastupdateddatetime DATETIME,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;