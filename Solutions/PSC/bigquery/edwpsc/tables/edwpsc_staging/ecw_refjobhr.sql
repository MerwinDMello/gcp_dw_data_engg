CREATE TABLE IF NOT EXISTS edwpsc_staging.ecw_refjobhr
(
  jobhrkey INT64 NOT NULL,
  jobcodehr STRING NOT NULL,
  jobdescriptionhr STRING NOT NULL,
  jobclasscodehr STRING NOT NULL,
  jobclassdescriptionhr STRING NOT NULL,
  dwlastupdatedatetime DATETIME NOT NULL,
  sourcesystemcode STRING NOT NULL,
  insertedby STRING NOT NULL,
  inserteddtm DATETIME NOT NULL,
  modifiedby STRING NOT NULL,
  modifieddtm DATETIME NOT NULL
)
;