CREATE OR REPLACE VIEW edwpsc_views.`epic_factclaimpayercpid`
AS SELECT
  `epic_factclaimpayercpid`.claimpayercpidkey,
  `epic_factclaimpayercpid`.claimkey,
  `epic_factclaimpayercpid`.iplankey,
  `epic_factclaimpayercpid`.cpid,
  `epic_factclaimpayercpid`.regionkey,
  `epic_factclaimpayercpid`.sourceaprimarykeyvalue,
  `epic_factclaimpayercpid`.sourcebprimarykeyvalue,
  `epic_factclaimpayercpid`.dwlastupdatedatetime,
  `epic_factclaimpayercpid`.sourcesystemcode,
  `epic_factclaimpayercpid`.insertedby,
  `epic_factclaimpayercpid`.inserteddtm,
  `epic_factclaimpayercpid`.modifiedby,
  `epic_factclaimpayercpid`.modifieddtm
  FROM
    edwpsc_base_views.`epic_factclaimpayercpid`
;