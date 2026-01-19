CREATE OR REPLACE VIEW edwpsc_views.`epic_factworkqueuepatienterrors`
AS SELECT
  `epic_factworkqueuepatienterrors`.regionkey,
  `epic_factworkqueuepatienterrors`.cerid,
  `epic_factworkqueuepatienterrors`.ruleid,
  `epic_factworkqueuepatienterrors`.errormessage,
  `epic_factworkqueuepatienterrors`.deferdate,
  `epic_factworkqueuepatienterrors`.sourceaprimarykeyvalue,
  `epic_factworkqueuepatienterrors`.sourcebprimarykeyvalue,
  `epic_factworkqueuepatienterrors`.insertedby,
  `epic_factworkqueuepatienterrors`.inserteddtm,
  `epic_factworkqueuepatienterrors`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`epic_factworkqueuepatienterrors`
;