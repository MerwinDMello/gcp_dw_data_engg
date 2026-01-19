CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeecmsgs`
AS SELECT
  `artiva_stgpspeecmsgs`.pspeecmsgcnt,
  `artiva_stgpspeecmsgs`.pspeecmsgdate,
  `artiva_stgpspeecmsgs`.pspeecmsgectid,
  `artiva_stgpspeecmsgs`.pspeecmsgeditor,
  `artiva_stgpspeecmsgs`.pspeecmsgexportdte,
  `artiva_stgpspeecmsgs`.pspeecmsgid,
  `artiva_stgpspeecmsgs`.pspeecmsgnew,
  `artiva_stgpspeecmsgs`.pspeecmsgportalid,
  `artiva_stgpspeecmsgs`.pspeecmsgsource,
  `artiva_stgpspeecmsgs`.pspeecmsgtext,
  `artiva_stgpspeecmsgs`.pspeecmsgtime,
  `artiva_stgpspeecmsgs`.pspeecmsgtype,
  `artiva_stgpspeecmsgs`.pspeecmsguser,
  `artiva_stgpspeecmsgs`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpspeecmsgs`
;