CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspeactionhist`
AS SELECT
  `artiva_stgpspeactionhist`.pspeacthactid,
  `artiva_stgpspeactionhist`.pspeacthaviid,
  `artiva_stgpspeactionhist`.pspeacthdte,
  `artiva_stgpspeactionhist`.pspeacthid,
  `artiva_stgpspeactionhist`.pspeacthnote,
  `artiva_stgpspeactionhist`.pspeacthnoteline,
  `artiva_stgpspeactionhist`.pspeacthperfid,
  `artiva_stgpspeactionhist`.pspeacthpoolid,
  `artiva_stgpspeactionhist`.pspeacthppiid,
  `artiva_stgpspeactionhist`.pspeacthresid,
  `artiva_stgpspeactionhist`.pspeacthstat,
  `artiva_stgpspeactionhist`.pspeacthtime,
  `artiva_stgpspeactionhist`.pspeacthuserid,
  `artiva_stgpspeactionhist`.lastnote,
  `artiva_stgpspeactionhist`.lastnotecnt,
  `artiva_stgpspeactionhist`.lastnoteuserid,
  `artiva_stgpspeactionhist`.lastnotedatetime,
  `artiva_stgpspeactionhist`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpspeactionhist`
;