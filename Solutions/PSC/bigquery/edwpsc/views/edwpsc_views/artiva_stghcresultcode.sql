CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcresultcode`
AS SELECT
  `artiva_stghcresultcode`.hcresactgrpid,
  `artiva_stghcresultcode`.hcrescallstat,
  `artiva_stghcresultcode`.hcrescontactstat,
  `artiva_stghcresultcode`.hcresdesc,
  `artiva_stghcresultcode`.hcresfollim,
  `artiva_stghcresultcode`.hcreshelpid,
  `artiva_stghcresultcode`.hcresid,
  `artiva_stghcresultcode`.hcresnewstat,
  `artiva_stghcresultcode`.hcresnotetext,
  `artiva_stghcresultcode`.hcresphase,
  `artiva_stghcresultcode`.hcresseccode,
  `artiva_stghcresultcode`.hcreswaitdate,
  `artiva_stghcresultcode`.hcreswkflws,
  `artiva_stghcresultcode`.hcresworked
  FROM
    edwpsc_base_views.`artiva_stghcresultcode`
;