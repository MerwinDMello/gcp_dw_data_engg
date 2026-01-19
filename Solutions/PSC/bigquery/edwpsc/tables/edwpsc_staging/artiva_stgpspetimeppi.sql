CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspetimeppi
(
  pspetimeaviid NUMERIC(29),
  pspetimedate DATETIME,
  pspetimeetime DATETIME,
  pspetimekey NUMERIC(29) NOT NULL,
  pspetimeperfid STRING,
  pspetimeppiid STRING,
  pspetimestime DATETIME,
  pspetimetime DATETIME,
  pspetimettime NUMERIC(31, 2),
  pspetimeuser STRING,
  pspetimeworked STRING,
  dwlastupdatedatetime DATETIME,
  PRIMARY KEY (pspetimekey) NOT ENFORCED
)
;