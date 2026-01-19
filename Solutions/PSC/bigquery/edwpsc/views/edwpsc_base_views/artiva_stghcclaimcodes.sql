CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghcclaimcodes`
AS SELECT
  `artiva_stghcclaimcodes`.hcclccid,
  `artiva_stghcclaimcodes`.hcclid,
  `artiva_stghcclaimcodes`.hcclldesc,
  `artiva_stghcclaimcodes`.hcclloaddate,
  `artiva_stghcclaimcodes`.hcclmodifieddate,
  `artiva_stghcclaimcodes`.hcclsdesc,
  `artiva_stghcclaimcodes`.hcclstartdate,
  `artiva_stghcclaimcodes`.hcclstopdate,
  `artiva_stghcclaimcodes`.psclactive,
  `artiva_stghcclaimcodes`.psclnteflg,
  `artiva_stghcclaimcodes`.psclufm,
  `artiva_stghcclaimcodes`.psclufmflg
  FROM
    edwpsc.`artiva_stghcclaimcodes`
;