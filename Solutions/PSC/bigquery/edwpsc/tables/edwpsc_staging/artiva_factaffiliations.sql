CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_factaffiliations
(
  artivaaffiliationskey INT64 NOT NULL,
  artivaproviderkey STRING,
  artivagroupkey STRING,
  artivainstitutionkey STRING,
  provideraffiliationlevel STRING,
  hospitalaffiliation STRING,
  affiliationstartdate DATE,
  affiliationtermdate DATE,
  affiliationtype STRING,
  affiliationstate STRING,
  hospitalaffiliationcategory STRING,
  affiliationspecialty STRING,
  affiliationactiveflag STRING,
  dwlastupdatedatetime DATETIME,
  sourcesystemcode STRING,
  insertedby STRING,
  inserteddtm DATETIME,
  modifiedby STRING,
  modifieddtm DATETIME,
  PRIMARY KEY (artivaaffiliationskey) NOT ENFORCED
)
;