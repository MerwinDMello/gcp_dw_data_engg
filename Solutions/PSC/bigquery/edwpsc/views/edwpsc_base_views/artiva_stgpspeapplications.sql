CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stgpspeapplications`
AS SELECT
  `artiva_stgpspeapplications`.pspeappactiongroup,
  `artiva_stgpspeapplications`.pspeappactive,
  `artiva_stgpspeapplications`.pspeappamtfield1,
  `artiva_stgpspeapplications`.pspeappamtfield2,
  `artiva_stgpspeapplications`.pspeappchrfield1,
  `artiva_stgpspeapplications`.pspeappchrfield10,
  `artiva_stgpspeapplications`.pspeappchrfield2,
  `artiva_stgpspeapplications`.pspeappchrfield3,
  `artiva_stgpspeapplications`.pspeappchrfield4,
  `artiva_stgpspeapplications`.pspeappchrfield5,
  `artiva_stgpspeapplications`.pspeappchrfield6,
  `artiva_stgpspeapplications`.pspeappchrfield7,
  `artiva_stgpspeapplications`.pspeappchrfield8,
  `artiva_stgpspeapplications`.pspeappchrfield9,
  `artiva_stgpspeapplications`.pspeappdesc,
  `artiva_stgpspeapplications`.pspeappdtefield1,
  `artiva_stgpspeapplications`.pspeappdtefield2,
  `artiva_stgpspeapplications`.pspeappenableins,
  `artiva_stgpspeapplications`.pspeappextfile,
  `artiva_stgpspeapplications`.pspeappkey
  FROM
    edwpsc.`artiva_stgpspeapplications`
;