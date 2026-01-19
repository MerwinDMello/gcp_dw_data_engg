CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeecreqtype`
AS SELECT
  `artiva_stgpspeecreqtype`.pspeecreqactgrp,
  `artiva_stgpspeecreqtype`.pspeecreqassignedto,
  `artiva_stgpspeecreqtype`.pspeecreqautogrp,
  `artiva_stgpspeecreqtype`.pspeecreqautoprov,
  `artiva_stgpspeecreqtype`.pspeecreqavitype,
  `artiva_stgpspeecreqtype`.pspeecreqcloseactgrp,
  `artiva_stgpspeecreqtype`.pspeecreqdesc,
  `artiva_stgpspeecreqtype`.pspeecreqesc1,
  `artiva_stgpspeecreqtype`.pspeecreqesc2,
  `artiva_stgpspeecreqtype`.pspeecreqfoecttype,
  `artiva_stgpspeecreqtype`.pspeecreqlocked,
  `artiva_stgpspeecreqtype`.pspeecreqppitype,
  `artiva_stgpspeecreqtype`.pspeecreqpri,
  `artiva_stgpspeecreqtype`.pspeecreqrte,
  `artiva_stgpspeecreqtype`.pspeecreqsigreq,
  `artiva_stgpspeecreqtype`.pspeecreqspanppi,
  `artiva_stgpspeecreqtype`.pspeecreqtyp,
  `artiva_stgpspeecreqtype`.pspeecrtid,
  `artiva_stgpspeecreqtype`.pspeecreqyc,
  `artiva_stgpspeecreqtype`.pspeecreqyf,
  `artiva_stgpspeecreqtype`.pspeecreqtypeind
  FROM
    edwpsc.`artiva_stgpspeecreqtype`
;