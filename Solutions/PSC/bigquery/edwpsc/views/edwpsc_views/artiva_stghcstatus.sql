CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcstatus`
AS SELECT
  `artiva_stghcstatus`.hcstacmsg,
  `artiva_stghcstatus`.hcstatid,
  `artiva_stghcstatus`.hcstbillable,
  `artiva_stghcstatus`.hcstdesc,
  `artiva_stghcstatus`.hcstesccd,
  `artiva_stghcstatus`.hcstescdays,
  `artiva_stghcstatus`.hcstescdayscript,
  `artiva_stghcstatus`.hcstescfilter,
  `artiva_stghcstatus`.hcstescscript,
  `artiva_stghcstatus`.hcstescstatscript,
  `artiva_stghcstatus`.hcstlinkflg,
  `artiva_stghcstatus`.hcstphase,
  `artiva_stghcstatus`.hcstupdrelaccts,
  `artiva_stghcstatus`.hcstworked
  FROM
    edwpsc_base_views.`artiva_stghcstatus`
;