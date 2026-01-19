CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpsocerrors`
AS SELECT
  `artiva_stgpsocerrors`.psocekey,
  `artiva_stgpsocerrors`.psoceactionid,
  `artiva_stgpsocerrors`.psoceactionname,
  `artiva_stgpsocerrors`.psocecategory,
  `artiva_stgpsocerrors`.psocecrosserror,
  `artiva_stgpsocerrors`.psocecrossroll,
  `artiva_stgpsocerrors`.psoceerrordte,
  `artiva_stgpsocerrors`.psoceerrorid,
  `artiva_stgpsocerrors`.psoceerrormessage,
  `artiva_stgpsocerrors`.psoceinerror,
  `artiva_stgpsocerrors`.psoceloaddte,
  `artiva_stgpsocerrors`.psocenotes,
  `artiva_stgpsocerrors`.psoceocid,
  `artiva_stgpsocerrors`.psoceorigdte,
  `artiva_stgpsocerrors`.psocersreasid,
  `artiva_stgpsocerrors`.psocersreason,
  `artiva_stgpsocerrors`.psocerstepid,
  `artiva_stgpsocerrors`.psocerstepname,
  `artiva_stgpsocerrors`.dwlastupdatedatetime
  FROM
    edwpsc.`artiva_stgpsocerrors`
;