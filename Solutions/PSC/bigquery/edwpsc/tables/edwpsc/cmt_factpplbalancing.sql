CREATE TABLE IF NOT EXISTS edwpsc.cmt_factpplbalancing
(
  pplbalancinglkey INT64 NOT NULL,
  transactiontype STRING,
  posteddate DATE,
  coid STRING,
  siteid INT64,
  postedsystemtypeid INT64,
  arsystem STRING,
  pplsumsummary STRING,
  totalamountpostedinecw NUMERIC(31, 2),
  totalamountpostedincmt NUMERIC(31, 2),
  variance NUMERIC(31, 2),
  specialist STRING,
  notes STRING,
  reasons STRING,
  dwlastupdatedatetime DATETIME
)
;