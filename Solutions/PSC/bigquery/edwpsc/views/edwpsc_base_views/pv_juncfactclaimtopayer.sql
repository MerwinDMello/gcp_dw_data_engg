CREATE OR REPLACE VIEW edwpsc_base_views.`pv_juncfactclaimtopayer`
AS SELECT
  `pv_juncfactclaimtopayer`.claimkey,
  `pv_juncfactclaimtopayer`.primaryclaimpayerkey,
  `pv_juncfactclaimtopayer`.secondaryclaimpayerkey,
  `pv_juncfactclaimtopayer`.tertiaryclaimpayerkey,
  `pv_juncfactclaimtopayer`.liabilityclaimpayerkey,
  `pv_juncfactclaimtopayer`.insertedby,
  `pv_juncfactclaimtopayer`.inserteddtm,
  `pv_juncfactclaimtopayer`.modifiedby,
  `pv_juncfactclaimtopayer`.modifieddtm,
  `pv_juncfactclaimtopayer`.dwlastupdatedatetime
  FROM
    edwpsc.`pv_juncfactclaimtopayer`
;