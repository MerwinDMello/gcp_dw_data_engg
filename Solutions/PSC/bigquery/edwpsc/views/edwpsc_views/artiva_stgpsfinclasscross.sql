CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpsfinclasscross`
AS SELECT
  `artiva_stgpsfinclasscross`.psfckey,
  `artiva_stgpsfinclasscross`.psfciplangroup,
  `artiva_stgpsfinclasscross`.psfcrossfincls,
  `artiva_stgpsfinclasscross`.psfcsource,
  `artiva_stgpsfinclasscross`.psfcsourcefincls,
  `artiva_stgpsfinclasscross`.psfcsourcetype,
  `artiva_stgpsfinclasscross`.insertedby,
  `artiva_stgpsfinclasscross`.inserteddtm,
  `artiva_stgpsfinclasscross`.modifiedby,
  `artiva_stgpsfinclasscross`.modifieddtm,
  `artiva_stgpsfinclasscross`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`artiva_stgpsfinclasscross`
;