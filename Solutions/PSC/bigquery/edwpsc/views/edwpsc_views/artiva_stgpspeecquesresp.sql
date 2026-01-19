CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeecquesresp`
AS SELECT
  `artiva_stgpspeecquesresp`.pspeecqrectid,
  `artiva_stgpspeecquesresp`.pspeecqrexportdte,
  `artiva_stgpspeecquesresp`.pspeecqrguid,
  `artiva_stgpspeecquesresp`.pspeecqrid,
  `artiva_stgpspeecquesresp`.pspeecqrloaddte,
  `artiva_stgpspeecquesresp`.pspeecqrppiid,
  `artiva_stgpspeecquesresp`.pspeecqrquestion,
  `artiva_stgpspeecquesresp`.pspeecqrquestiondte,
  `artiva_stgpspeecquesresp`.pspeecqrquestiontime,
  `artiva_stgpspeecquesresp`.pspeecqrquestionuser,
  `artiva_stgpspeecquesresp`.pspeecqrquestype,
  `artiva_stgpspeecquesresp`.pspeecqrresponse,
  `artiva_stgpspeecquesresp`.pspeecqrresponsedte,
  `artiva_stgpspeecquesresp`.pspeecqrresponsetime,
  `artiva_stgpspeecquesresp`.pspeecqrresponseuser,
  `artiva_stgpspeecquesresp`.pspeecqrsource,
  `artiva_stgpspeecquesresp`.pspeecqrsquestion,
  `artiva_stgpspeecquesresp`.pspeecqrsresponse
  FROM
    edwpsc_base_views.`artiva_stgpspeecquesresp`
;