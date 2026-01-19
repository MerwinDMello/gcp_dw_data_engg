CREATE OR REPLACE VIEW edwpsc_views.`epic_factworkqueueprebilledithistory`
AS SELECT
  `epic_factworkqueueprebilledithistory`.prebilledithistorykey,
  `epic_factworkqueueprebilledithistory`.regionkey,
  `epic_factworkqueueprebilledithistory`.claimkey,
  `epic_factworkqueueprebilledithistory`.encounterkey,
  `epic_factworkqueueprebilledithistory`.encounterid,
  `epic_factworkqueueprebilledithistory`.activitystartdate,
  `epic_factworkqueueprebilledithistory`.activityenddate,
  `epic_factworkqueueprebilledithistory`.activityname,
  `epic_factworkqueueprebilledithistory`.workqueueid,
  `epic_factworkqueueprebilledithistory`.workqueuename,
  `epic_factworkqueueprebilledithistory`.sourceaprimarykeyvalue,
  `epic_factworkqueueprebilledithistory`.sourcebprimarykeyvalue,
  `epic_factworkqueueprebilledithistory`.sourcesystemcode,
  `epic_factworkqueueprebilledithistory`.insertedby,
  `epic_factworkqueueprebilledithistory`.inserteddtm,
  `epic_factworkqueueprebilledithistory`.modifiedby,
  `epic_factworkqueueprebilledithistory`.modifieddtm,
  `epic_factworkqueueprebilledithistory`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factworkqueueprebilledithistory`
;