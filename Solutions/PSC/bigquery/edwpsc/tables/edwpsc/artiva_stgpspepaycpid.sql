CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspepaycpid
(
  pspepaycpcpidid STRING,
  pspepaycpkey NUMERIC(29) NOT NULL,
  pspepaycppayid STRING,
  PRIMARY KEY (pspepaycpkey) NOT ENFORCED
)
;