CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspetaxonomycodes
(
  pspetaxdesc STRING,
  pspetaxkey STRING NOT NULL,
  pspetaxmcrspccode STRING,
  pspetaxmcrsuptypdesc STRING,
  PRIMARY KEY (pspetaxkey) NOT ENFORCED
)
;