CREATE OR REPLACE VIEW edwpsc_views.`ecw_refagingbucket`
AS SELECT
  `ecw_refagingbucket`.ageindays,
  `ecw_refagingbucket`.bucketdescription1,
  `ecw_refagingbucket`.bucketdescription2,
  `ecw_refagingbucket`.bucketdescription3,
  `ecw_refagingbucket`.percnt,
  `ecw_refagingbucket`.percentbucket_0_10_gt100,
  `ecw_refagingbucket`.percentbucket_0_10_gt100sort,
  `ecw_refagingbucket`.agingbucket_0_30_nomax,
  `ecw_refagingbucket`.agingbucket_0_30_nomaxsort,
  `ecw_refagingbucket`.agingbucket_000_030_720plus,
  `ecw_refagingbucket`.agingbucket_000_030_720plussort,
  `ecw_refagingbucket`.agingbucket_a000_a030_nomax,
  `ecw_refagingbucket`.agingbucket_a000_a030_nomaxsort,
  `ecw_refagingbucket`.agingbucket_0_6_gt365,
  `ecw_refagingbucket`.agingbucket_0_6_gt365sort,
  `ecw_refagingbucket`.agingbucket_0_30_gt360,
  `ecw_refagingbucket`.agingbucket_0_30_gt360sort
  FROM
    edwpsc_base_views.`ecw_refagingbucket`
;