CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpspeecreqtype
(
  pspeecreqactgrp STRING,
  pspeecreqassignedto STRING,
  pspeecreqautogrp STRING,
  pspeecreqautoprov STRING,
  pspeecreqavitype STRING,
  pspeecreqcloseactgrp STRING,
  pspeecreqdesc STRING,
  pspeecreqesc1 STRING,
  pspeecreqesc2 STRING,
  pspeecreqfoecttype STRING,
  pspeecreqlocked STRING,
  pspeecreqppitype STRING,
  pspeecreqpri STRING,
  pspeecreqrte STRING,
  pspeecreqsigreq STRING,
  pspeecreqspanppi STRING,
  pspeecreqtyp STRING,
  pspeecrtid STRING NOT NULL,
  pspeecreqyc STRING,
  pspeecreqyf STRING,
  pspeecreqtypeind STRING,
  PRIMARY KEY (pspeecrtid) NOT ENFORCED
)
;