CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeinstitution`
AS SELECT
  `artiva_stgpspeinstitution`.pspeinaddr1,
  `artiva_stgpspeinstitution`.pspeinaddr2,
  `artiva_stgpspeinstitution`.pspeinaffiliations,
  `artiva_stgpspeinstitution`.pspeinboards,
  `artiva_stgpspeinstitution`.pspeincity,
  `artiva_stgpspeinstitution`.pspeincontact,
  `artiva_stgpspeinstitution`.pspeincountry,
  `artiva_stgpspeinstitution`.pspeineducation,
  `artiva_stgpspeinstitution`.pspeinfax,
  `artiva_stgpspeinstitution`.pspeinhomepgurl,
  `artiva_stgpspeinstitution`.pspeinid,
  `artiva_stgpspeinstitution`.pspeininsurance,
  `artiva_stgpspeinstitution`.pspeinlicenses,
  `artiva_stgpspeinstitution`.pspeinkey,
  `artiva_stgpspeinstitution`.pspeinname,
  `artiva_stgpspeinstitution`.pspeinplans,
  `artiva_stgpspeinstitution`.pspeinprimphone,
  `artiva_stgpspeinstitution`.pspeinst,
  `artiva_stgpspeinstitution`.pspeintype,
  `artiva_stgpspeinstitution`.pspeinzip
  FROM
    edwpsc.`artiva_stgpspeinstitution`
;