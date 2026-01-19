CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_rprtopenietsummarybycoid`
AS SELECT
  `ecw_rprtopenietsummarybycoid`.snapshotdate,
  `ecw_rprtopenietsummarybycoid`.groupname,
  `ecw_rprtopenietsummarybycoid`.divisionname,
  `ecw_rprtopenietsummarybycoid`.marketname,
  `ecw_rprtopenietsummarybycoid`.coid,
  `ecw_rprtopenietsummarybycoid`.coidname,
  `ecw_rprtopenietsummarybycoid`.claimnumber,
  `ecw_rprtopenietsummarybycoid`.claimbalance,
  `ecw_rprtopenietsummarybycoid`.errorcount,
  `ecw_rprtopenietsummarybycoid`.claimkey
  FROM
    edwpsc.`ecw_rprtopenietsummarybycoid`
;