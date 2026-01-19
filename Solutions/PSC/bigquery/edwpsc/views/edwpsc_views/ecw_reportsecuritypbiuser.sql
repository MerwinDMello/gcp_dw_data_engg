CREATE OR REPLACE VIEW edwpsc_views.`ecw_reportsecuritypbiuser`
AS SELECT
  `ecw_reportsecuritypbiuser`.fulluser34,
  `ecw_reportsecuritypbiuser`.user34,
  `ecw_reportsecuritypbiuser`.userpbi,
  `ecw_reportsecuritypbiuser`.employeetype,
  `ecw_reportsecuritypbiuser`.roleid,
  `ecw_reportsecuritypbiuser`.securecoidasuser34,
  `ecw_reportsecuritypbiuser`.secureemployeetypeasuser34,
  `ecw_reportsecuritypbiuser`.secureallasone,
  `ecw_reportsecuritypbiuser`.dwlastupdatedatetime
  FROM
    edwpsc_base_views.`ecw_reportsecuritypbiuser`
;