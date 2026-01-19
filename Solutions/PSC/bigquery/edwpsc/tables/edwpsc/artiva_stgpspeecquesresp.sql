CREATE TABLE IF NOT EXISTS edwpsc.artiva_stgpspeecquesresp
(
  pspeecqrectid STRING,
  pspeecqrexportdte DATETIME,
  pspeecqrguid STRING,
  pspeecqrid NUMERIC(29) NOT NULL,
  pspeecqrloaddte DATETIME,
  pspeecqrppiid STRING,
  pspeecqrquestion STRING,
  pspeecqrquestiondte DATETIME,
  pspeecqrquestiontime DATETIME,
  pspeecqrquestionuser STRING,
  pspeecqrquestype STRING,
  pspeecqrresponse STRING,
  pspeecqrresponsedte DATETIME,
  pspeecqrresponsetime DATETIME,
  pspeecqrresponseuser STRING,
  pspeecqrsource STRING,
  pspeecqrsquestion STRING,
  pspeecqrsresponse STRING,
  PRIMARY KEY (pspeecqrid) NOT ENFORCED
)
;