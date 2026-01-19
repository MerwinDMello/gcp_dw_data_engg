CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeppiassignments`
AS SELECT
  `artiva_stgpspeppiassignments`.pspeasgaddpayid,
  `artiva_stgpspeppiassignments`.pspeasgassoccoid,
  `artiva_stgpspeppiassignments`.pspeasgassoctaxid,
  `artiva_stgpspeppiassignments`.pspeasggafid,
  `artiva_stgpspeppiassignments`.pspeasggin,
  `artiva_stgpspeppiassignments`.pspeasgkey,
  `artiva_stgpspeppiassignments`.pspeasgorigenasgkey,
  `artiva_stgpspeppiassignments`.pspeasgpayorid,
  `artiva_stgpspeppiassignments`.pspeasgperfid,
  `artiva_stgpspeppiassignments`.pspeasgphysaddr,
  `artiva_stgpspeppiassignments`.pspeasgstdte
  FROM
    edwpsc.`artiva_stgpspeppiassignments`
;