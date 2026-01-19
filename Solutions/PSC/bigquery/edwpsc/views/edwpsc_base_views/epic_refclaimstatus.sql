CREATE OR REPLACE VIEW edwpsc_base_views.`epic_refclaimstatus`
AS SELECT
  `epic_refclaimstatus`.claimstatuskey,
  `epic_refclaimstatus`.claimstatusname,
  `epic_refclaimstatus`.claimstatusshortname,
  `epic_refclaimstatus`.regionkey,
  `epic_refclaimstatus`.activityc,
  `epic_refclaimstatus`.sourceprimarykeyvalue,
  `epic_refclaimstatus`.sourcerecordlastupdated,
  `epic_refclaimstatus`.dwlastupdatedatetime,
  `epic_refclaimstatus`.sourcesystemcode,
  `epic_refclaimstatus`.insertedby,
  `epic_refclaimstatus`.inserteddtm,
  `epic_refclaimstatus`.modifiedby,
  `epic_refclaimstatus`.modifieddtm,
  `epic_refclaimstatus`.deleteflag
  FROM
    edwpsc.`epic_refclaimstatus`
;