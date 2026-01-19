CREATE OR REPLACE VIEW edwpsc_base_views.`ecw_refcptanesthesiacmsoverride`
AS SELECT
  `ecw_refcptanesthesiacmsoverride`.cptcode,
  `ecw_refcptanesthesiacmsoverride`.cmsbaseunit
  FROM
    edwpsc.`ecw_refcptanesthesiacmsoverride`
;