CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghcactioncode`
AS SELECT
  `artiva_stghcactioncode`.hcactactgrpid,
  `artiva_stghcactioncode`.hcactcallstat,
  `artiva_stghcactioncode`.hcactcontactstat,
  `artiva_stghcactioncode`.hcactdesc,
  `artiva_stghcactioncode`.hcactfollim,
  `artiva_stghcactioncode`.hcacthelpid,
  `artiva_stghcactioncode`.hcactid,
  `artiva_stghcactioncode`.hcactnewstat,
  `artiva_stghcactioncode`.hcactnotetext,
  `artiva_stghcactioncode`.hcactphase,
  `artiva_stghcactioncode`.hcactrescnt,
  `artiva_stghcactioncode`.hcactseccode,
  `artiva_stghcactioncode`.hcactwaitdate,
  `artiva_stghcactioncode`.hcactwkflws,
  `artiva_stghcactioncode`.hcactworked
  FROM
    edwpsc.`artiva_stghcactioncode`
;