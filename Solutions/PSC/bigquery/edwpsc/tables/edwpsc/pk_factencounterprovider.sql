CREATE TABLE IF NOT EXISTS edwpsc.pk_factencounterprovider
(
  pkencounterproviderkey INT64 NOT NULL,
  pkregionname STRING,
  pkencounterkey INT64,
  encounterid INT64,
  begineffectivedate DATETIME,
  endeffectivedate DATETIME,
  providertype STRING,
  providerlastname STRING,
  providerfirstname STRING,
  providermiddlename STRING,
  providernpi STRING,
  deleteflag INT64,
  dwlastupdatedatetime DATETIME,
  sourceaprimarykeyvalue INT64,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  pkfinancialnumber STRING,
  personid INT64
)
;