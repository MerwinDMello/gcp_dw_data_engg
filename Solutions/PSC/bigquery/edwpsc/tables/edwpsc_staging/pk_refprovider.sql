CREATE TABLE IF NOT EXISTS edwpsc_staging.pk_refprovider
(
  pkproviderkey INT64 NOT NULL,
  providerusername STRING,
  providerdomusername STRING,
  providerfirstname STRING,
  providerlastname STRING,
  providermiddlename STRING,
  providerfullname STRING,
  providerdeanumber STRING,
  providernpi STRING,
  authindicatorflag INT64,
  deleteflag INT64,
  createddatetime DATETIME,
  updateddatetime DATETIME,
  pkregionname STRING,
  sourceaprimarykeyvalue INT64,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  dwlastupdatedatetime DATETIME
)
;