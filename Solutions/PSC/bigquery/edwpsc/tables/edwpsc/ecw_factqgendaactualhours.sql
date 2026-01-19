CREATE TABLE IF NOT EXISTS edwpsc.ecw_factqgendaactualhours
(
  qgendaprovideractualhourskey INT64 NOT NULL,
  provideractualhoursk NUMERIC(29) NOT NULL,
  coid STRING,
  companycode STRING,
  locationsk NUMERIC(29),
  providerscheduledtypedesc STRING,
  providershifttypedesc STRING,
  providerstaffcategorydesc STRING,
  lastname STRING,
  firstname STRING,
  npi STRING,
  providerstafftypedesc STRING,
  provideractualstartdatetime DATETIME,
  provideractualenddatetime DATETIME,
  provideractualminuteqty INT64,
  providereffectivestartdatetime DATETIME,
  providereffectiveenddatetime DATETIME,
  providereffectiveminuteqty INT64,
  deletedflag INT64 NOT NULL,
  sourcesystemcode STRING,
  dwlastupdatedatetime DATETIME NOT NULL,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;