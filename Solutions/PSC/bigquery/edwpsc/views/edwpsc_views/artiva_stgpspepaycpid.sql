CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspepaycpid`
AS SELECT
  `artiva_stgpspepaycpid`.pspepaycpcpidid,
  `artiva_stgpspepaycpid`.pspepaycpkey,
  `artiva_stgpspepaycpid`.pspepaycppayid
  FROM
    edwpsc_base_views.`artiva_stgpspepaycpid`
;