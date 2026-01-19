CREATE OR REPLACE VIEW edwpsc_views.`artiva_stgpspelocation`
AS SELECT
  `artiva_stgpspelocation`.pspelocaddr1,
  `artiva_stgpspelocation`.pspelocaddr2,
  `artiva_stgpspelocation`.pspelocaltext,
  `artiva_stgpspelocation`.pspeloccity,
  `artiva_stgpspelocation`.pspeloccounty,
  `artiva_stgpspelocation`.pspelocdesc,
  `artiva_stgpspelocation`.pspelocecwfacid,
  `artiva_stgpspelocation`.pspelocfax,
  `artiva_stgpspelocation`.pspelocfulladdr,
  `artiva_stgpspelocation`.pspelockey,
  `artiva_stgpspelocation`.pspelocmgremail,
  `artiva_stgpspelocation`.pspelocoffmgrname,
  `artiva_stgpspelocation`.pspelocphone,
  `artiva_stgpspelocation`.pspelocstate,
  `artiva_stgpspelocation`.pspeloctaxid,
  `artiva_stgpspelocation`.pspeloczip
  FROM
    edwpsc_base_views.`artiva_stgpspelocation`
;