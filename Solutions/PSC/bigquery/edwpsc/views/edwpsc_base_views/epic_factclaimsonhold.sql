CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factclaimsonhold`
AS SELECT
  `epic_factclaimsonhold`.claimsonholdkey,
  `epic_factclaimsonhold`.loaddatekey,
  `epic_factclaimsonhold`.claimkey,
  `epic_factclaimsonhold`.claimnumber,
  `epic_factclaimsonhold`.regionkey,
  `epic_factclaimsonhold`.holdcodekey,
  `epic_factclaimsonhold`.holdcode,
  `epic_factclaimsonhold`.holdcodename,
  `epic_factclaimsonhold`.holdfromdatekey,
  `epic_factclaimsonhold`.holdtodatekey,
  `epic_factclaimsonhold`.daysonhold,
  `epic_factclaimsonhold`.dwlastupdatedatetime,
  `epic_factclaimsonhold`.sourcesystemcode,
  `epic_factclaimsonhold`.insertedby,
  `epic_factclaimsonhold`.inserteddtm,
  `epic_factclaimsonhold`.modifiedby,
  `epic_factclaimsonhold`.modifieddtm
  FROM
    edwpsc.`epic_factclaimsonhold`
;