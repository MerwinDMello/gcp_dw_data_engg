CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_juncfactclaimtopayer`
AS SELECT
  `ecw_juncfactclaimtopayer`.claimkey,
  `ecw_juncfactclaimtopayer`.primaryclaimpayerkey,
  `ecw_juncfactclaimtopayer`.secondaryclaimpayerkey,
  `ecw_juncfactclaimtopayer`.tertiaryclaimpayerkey,
  `ecw_juncfactclaimtopayer`.liabilityclaimpayerkey,
  `ecw_juncfactclaimtopayer`.insertedby,
  `ecw_juncfactclaimtopayer`.inserteddtm,
  `ecw_juncfactclaimtopayer`.modifiedby,
  `ecw_juncfactclaimtopayer`.modifieddtm,
  `ecw_juncfactclaimtopayer`.dwlastupdatedatetime
  FROM
    edwpsc.`ecw_juncfactclaimtopayer`
;