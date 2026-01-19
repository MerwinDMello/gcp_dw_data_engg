CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeppilinks
(
  pspeliaviid NUMERIC(29),
  pspelicount STRING,
  pspelidate DATETIME,
  pspelidesc STRING,
  pspelikey NUMERIC(29) NOT NULL,
  pspelileadppi NUMERIC(29),
  pspeliprod STRING,
  pspeliuser STRING,
  pspeliuserchgdte DATETIME,
  PRIMARY KEY (pspelikey) NOT ENFORCED
)
;