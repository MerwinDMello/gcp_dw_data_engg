CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_factencountertochargeproviderrole
(
  encountertochargeproviderrole INT64 NOT NULL,
  encountertochargekey INT64 NOT NULL,
  providerrole STRING,
  providername STRING,
  facilitymnemonic STRING,
  providermnemonic STRING,
  providernpi STRING,
  providerdealicensenumber STRING,
  pscproviderflag INT64,
  assigneddtm DATETIME,
  dwlastupdatedatetime DATETIME,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME
)
;