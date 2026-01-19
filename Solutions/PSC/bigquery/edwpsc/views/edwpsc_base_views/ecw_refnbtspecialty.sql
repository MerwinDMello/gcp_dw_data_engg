CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refnbtspecialty`
AS SELECT
  `ecw_refnbtspecialty`.nbtspecialtyidkey,
  `ecw_refnbtspecialty`.specialtyid,
  `ecw_refnbtspecialty`.specialtycode,
  `ecw_refnbtspecialty`.specialtydescription,
  `ecw_refnbtspecialty`.specialtytypeid,
  `ecw_refnbtspecialty`.specialtyactive,
  `ecw_refnbtspecialty`.specialtyuuid,
  `ecw_refnbtspecialty`.nbtspecialtycategoryidkey,
  `ecw_refnbtspecialty`.dwlastupdatedatetime,
  `ecw_refnbtspecialty`.sourcesystemcode,
  `ecw_refnbtspecialty`.insertedby,
  `ecw_refnbtspecialty`.inserteddtm,
  `ecw_refnbtspecialty`.modifiedby,
  `ecw_refnbtspecialty`.modifieddtm
  FROM
    edwpsc.`ecw_refnbtspecialty`
;