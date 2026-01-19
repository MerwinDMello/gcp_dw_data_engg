CREATE OR REPLACE VIEW edwpsc_base_views.`epic_juncfactclaimtopayer`
AS SELECT
  `epic_juncfactclaimtopayer`.claimkey,
  `epic_juncfactclaimtopayer`.primaryclaimpayerkey,
  `epic_juncfactclaimtopayer`.secondaryclaimpayerkey,
  `epic_juncfactclaimtopayer`.tertiaryclaimpayerkey,
  `epic_juncfactclaimtopayer`.liabilityclaimpayerkey,
  `epic_juncfactclaimtopayer`.insertedby,
  `epic_juncfactclaimtopayer`.inserteddtm,
  `epic_juncfactclaimtopayer`.modifiedby,
  `epic_juncfactclaimtopayer`.modifieddtm,
  `epic_juncfactclaimtopayer`.dwlastupdatedatetime
  FROM
    edwpsc.`epic_juncfactclaimtopayer`
;