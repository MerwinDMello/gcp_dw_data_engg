CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factsnapshotclaimsonhold`
AS SELECT
  `epic_factsnapshotclaimsonhold`.monthid,
  `epic_factsnapshotclaimsonhold`.snapshotdate,
  `epic_factsnapshotclaimsonhold`.loaddatekey,
  `epic_factsnapshotclaimsonhold`.claimkey,
  `epic_factsnapshotclaimsonhold`.claimnumber,
  `epic_factsnapshotclaimsonhold`.regionkey,
  `epic_factsnapshotclaimsonhold`.holdcodekey,
  `epic_factsnapshotclaimsonhold`.holdcode,
  `epic_factsnapshotclaimsonhold`.holdcodename,
  `epic_factsnapshotclaimsonhold`.holdfromdatekey,
  `epic_factsnapshotclaimsonhold`.holdtodatekey,
  `epic_factsnapshotclaimsonhold`.daysonhold,
  `epic_factsnapshotclaimsonhold`.sourcesystemcode,
  `epic_factsnapshotclaimsonhold`.insertedby,
  `epic_factsnapshotclaimsonhold`.inserteddtm
  FROM
    edwpsc.`epic_factsnapshotclaimsonhold`
;