CREATE OR REPLACE VIEW edwpsc_base_views.`epic_factencounternotes`
AS SELECT
  `epic_factencounternotes`.encounternoteskey,
  `epic_factencounternotes`.regionkey,
  `epic_factencounternotes`.encounterkey,
  `epic_factencounternotes`.encounterid,
  `epic_factencounternotes`.claimkey,
  `epic_factencounternotes`.visitnumber,
  `epic_factencounternotes`.notetype,
  `epic_factencounternotes`.notedatekey,
  `epic_factencounternotes`.noteuserkey,
  `epic_factencounternotes`.encounternote,
  `epic_factencounternotes`.servicingproviderkey,
  `epic_factencounternotes`.paytoproviderkey,
  `epic_factencounternotes`.patientkey,
  `epic_factencounternotes`.serviceareakey,
  `epic_factencounternotes`.sourceaprimarykeyvalue,
  `epic_factencounternotes`.sourcebprimarykeyvalue,
  `epic_factencounternotes`.dwlastupdatedatetime,
  `epic_factencounternotes`.sourcesystemcode,
  `epic_factencounternotes`.insertedby,
  `epic_factencounternotes`.inserteddtm,
  `epic_factencounternotes`.modifiedby,
  `epic_factencounternotes`.modifieddtm
  FROM
    edwpsc.`epic_factencounternotes`
;