CREATE TABLE IF NOT EXISTS edwpsc_staging.artiva_stgpsocerrors
(
  psocekey INT64,
  psoceactionid STRING,
  psoceactionname STRING,
  psocecategory STRING,
  psocecrosserror STRING,
  psocecrossroll STRING,
  psoceerrordte DATETIME,
  psoceerrorid STRING,
  psoceerrormessage STRING,
  psoceinerror STRING,
  psoceloaddte DATETIME,
  psocenotes STRING,
  psoceocid INT64,
  psoceorigdte DATETIME,
  psocersreasid STRING,
  psocersreason STRING,
  psocerstepid STRING,
  psocerstepname STRING,
  dwlastupdatedatetime DATETIME
)
;