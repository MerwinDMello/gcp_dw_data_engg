CREATE OR REPLACE VIEW edwpsc_base_views.`artiva_stghcclcategory`
AS SELECT
  `artiva_stghcclcategory`.hccadesc,
  `artiva_stghcclcategory`.hccaid
  FROM
    edwpsc.`artiva_stghcclcategory`
;