CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsratereference
(
  psrcoid STRING,
  psrdept STRING,
  psrfinclass STRING,
  psrkey STRING NOT NULL,
  psrprocflg STRING,
  psrrate STRING,
  PRIMARY KEY (psrkey) NOT ENFORCED
)
;