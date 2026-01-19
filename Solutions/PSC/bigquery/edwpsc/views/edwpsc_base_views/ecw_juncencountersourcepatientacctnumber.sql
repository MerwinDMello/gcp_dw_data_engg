CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncencountersourcepatientacctnumber`
AS SELECT
  `ecw_juncencountersourcepatientacctnumber`.encsourcepatacctnumberkey,
  `ecw_juncencountersourcepatientacctnumber`.regionkey,
  `ecw_juncencountersourcepatientacctnumber`.encounterid,
  `ecw_juncencountersourcepatientacctnumber`.sourcepatientacctnumber,
  `ecw_juncencountersourcepatientacctnumber`.insertedby,
  `ecw_juncencountersourcepatientacctnumber`.inserteddtm,
  `ecw_juncencountersourcepatientacctnumber`.modifiedby,
  `ecw_juncencountersourcepatientacctnumber`.modifieddtm,
  `ecw_juncencountersourcepatientacctnumber`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncencountersourcepatientacctnumber`
;