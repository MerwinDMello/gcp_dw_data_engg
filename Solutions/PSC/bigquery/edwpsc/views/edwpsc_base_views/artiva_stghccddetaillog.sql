CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghccddetaillog`
AS SELECT
  `artiva_stghccddetaillog`.hccdacid,
  `artiva_stghccddetaillog`.hccdclcat,
  `artiva_stghccddetaillog`.hccdcldte,
  `artiva_stghccddetaillog`.hccdclscode,
  `artiva_stghccddetaillog`.hccdclsubcat,
  `artiva_stghccddetaillog`.hccdcompdte,
  `artiva_stghccddetaillog`.hccdconsvy,
  `artiva_stghccddetaillog`.hccdcss,
  `artiva_stghccddetaillog`.hccddatasrc,
  `artiva_stghccddetaillog`.hccdenid,
  `artiva_stghccddetaillog`.hccdexpdte,
  `artiva_stghccddetaillog`.hccdfacid,
  `artiva_stghccddetaillog`.hccdfinclass,
  `artiva_stghccddetaillog`.hccdid,
  `artiva_stghccddetaillog`.hccdmpayerid,
  `artiva_stghccddetaillog`.hccdpayerid,
  `artiva_stghccddetaillog`.hccdreqdte,
  `artiva_stghccddetaillog`.hccdstatus,
  `artiva_stghccddetaillog`.hccdtype,
  `artiva_stghccddetaillog`.hccdversion,
  `artiva_stghccddetaillog`.pscdnoiet,
  `artiva_stghccddetaillog`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stghccddetaillog`
;