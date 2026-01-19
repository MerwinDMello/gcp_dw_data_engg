CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspooladdinfo`
AS SELECT
  `artiva_stgpspooladdinfo`.pool,
  `artiva_stgpspooladdinfo`.pspooldept,
  `artiva_stgpspooladdinfo`.pspooltype,
  `artiva_stgpspooladdinfo`.poolsubgroup,
  `artiva_stgpspooladdinfo`.poolfunction,
  `artiva_stgpspooladdinfo`.poolsla,
  `artiva_stgpspooladdinfo`.pspoolvendor,
  `artiva_stgpspooladdinfo`.pspoolitmprhr,
  `artiva_stgpspooladdinfo`.pspoolvndesc,
  `artiva_stgpspooladdinfo`.pspoollob,
  `artiva_stgpspooladdinfo`.pspoolmanager,
  `artiva_stgpspooladdinfo`.pspoolpaygrp,
  `artiva_stgpspooladdinfo`.pspoolresdept,
  `artiva_stgpspooladdinfo`.pspooltier,
  `artiva_stgpspooladdinfo`.pspoolescpoolflg,
  `artiva_stgpspooladdinfo`.pspoolconctrlnum,
  `artiva_stgpspooladdinfo`.pspooleffectdte,
  `artiva_stgpspooladdinfo`.pspoollaborstdname
  FROM
    edwpsc.`artiva_stgpspooladdinfo`
;