CREATE OR REPLACE VIEW edwpsc_base_views.`cmt_factpplbalancing`
AS SELECT
  `cmt_factpplbalancing`.pplbalancinglkey,
  `cmt_factpplbalancing`.transactiontype,
  `cmt_factpplbalancing`.posteddate,
  `cmt_factpplbalancing`.coid,
  `cmt_factpplbalancing`.siteid,
  `cmt_factpplbalancing`.postedsystemtypeid,
  `cmt_factpplbalancing`.arsystem,
  `cmt_factpplbalancing`.pplsumsummary,
  `cmt_factpplbalancing`.totalamountpostedinecw,
  `cmt_factpplbalancing`.totalamountpostedincmt,
  `cmt_factpplbalancing`.variance,
  `cmt_factpplbalancing`.specialist,
  `cmt_factpplbalancing`.notes,
  `cmt_factpplbalancing`.reasons,
  `cmt_factpplbalancing`.dwlastupdatedatetime
  FROM
    edwpsc.`cmt_factpplbalancing`
;