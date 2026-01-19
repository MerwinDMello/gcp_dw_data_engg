CREATE OR REPLACE VIEW edwpsc_views.`artiva_stghcclsubcategory`
AS SELECT
  `artiva_stghcclsubcategory`.hcccactgrp,
  `artiva_stghcclsubcategory`.hccccaid,
  `artiva_stghcclsubcategory`.hcccid,
  `artiva_stghcclsubcategory`.hcccldesc,
  `artiva_stghcclsubcategory`.hcccloaddate,
  `artiva_stghcclsubcategory`.hcccmodifieddate,
  `artiva_stghcclsubcategory`.hcccsdesc,
  `artiva_stghcclsubcategory`.hcccstartdate,
  `artiva_stghcclsubcategory`.hcccstopdate,
  `artiva_stghcclsubcategory`.pscccstyp,
  `artiva_stghcclsubcategory`.psccfollowupsubcat,
  `artiva_stghcclsubcategory`.pscciet,
  `artiva_stghcclsubcategory`.psccpriority,
  `artiva_stghcclsubcategory`.psccsource
  FROM
    edwpsc_base_views.`artiva_stghcclsubcategory`
;