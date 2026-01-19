CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspepraltproviders`
AS SELECT
  `artiva_stgpspepraltproviders`.pspepraltactiveind,
  `artiva_stgpspepraltproviders`.pspepraltdtefrm,
  `artiva_stgpspepraltproviders`.pspepraltdteto,
  `artiva_stgpspepraltproviders`.pspepraltgafid,
  `artiva_stgpspepraltproviders`.pspepraltid,
  `artiva_stgpspepraltproviders`.pspepraltkey,
  `artiva_stgpspepraltproviders`.pspepraltlongnm,
  `artiva_stgpspepraltproviders`.pspepraltperfid,
  `artiva_stgpspepraltproviders`.pspepraltprovid,
  `artiva_stgpspepraltproviders`.pspepralttype
  FROM
    edwpsc.`artiva_stgpspepraltproviders`
;