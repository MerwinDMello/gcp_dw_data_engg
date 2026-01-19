CREATE OR REPLACE VIEW edwpsc_views.`ecw_refcptanesthesiacmsoverride`
AS SELECT
  `ecw_refcptanesthesiacmsoverride`.cptcode,
  `ecw_refcptanesthesiacmsoverride`.cmsbaseunit
  FROM
    edwpsc_base_views.`ecw_refcptanesthesiacmsoverride`
;