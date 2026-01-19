CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgstuser
(
  userid STRING NOT NULL,
  uafirst STRING,
  ualast STRING,
  uafullname STRING,
  hcuatitle STRING,
  hcuadept STRING,
  uaoffice STRING,
  uassodomain STRING,
  PRIMARY KEY (userid) NOT ENFORCED
)
;