-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_rcm_age_group.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_age_group AS SELECT
    eis_age_dim.age_month_sid,
    eis_age_dim.age_member,
    eis_age_dim.age_alias,
    CASE
      WHEN eis_age_dim.age_member IN(
        '0-2', '3-5', '6-10', '11-15', '16-20', '21-25', '26-30', 'Billed_Under_31_Days'
      ) THEN '11'
      WHEN eis_age_dim.age_member IN(
        '31-60'
      ) THEN '13'
      WHEN eis_age_dim.age_member IN(
        '61-90'
      ) THEN '14'
      WHEN eis_age_dim.age_member IN(
        '91-120'
      ) THEN '15'
      WHEN eis_age_dim.age_member IN(
        '121-150'
      ) THEN '16'
      WHEN eis_age_dim.age_member IN(
        '151-180'
      ) THEN '17'
      WHEN eis_age_dim.age_member IN(
        '181-210', '211-240', '241-270', '271-300', '301-330', '331-360', 'Over_360'
      ) THEN '18'
      WHEN eis_age_dim.age_member IN(
        'Unbilled_31_Plus_Day'
      ) THEN '12'
      ELSE '99'
    END AS age_group_code,
    CASE
      WHEN eis_age_dim.age_member IN(
        '0-2', '3-5', '6-10', '11-15', '16-20', '21-25', '26-30', 'Billed_Under_31_Days'
      ) THEN '0-30'
      WHEN eis_age_dim.age_member IN(
        '31-60', '61-90', '91-120', '121-150', '151-180'
      ) THEN eis_age_dim.age_member
      WHEN eis_age_dim.age_member IN(
        '181-210', '211-240', '241-270', '271-300', '301-330', '331-360', 'Over_360'
      ) THEN '181+'
      WHEN eis_age_dim.age_member IN(
        'Unbilled_31_Plus_Day'
      ) THEN 'Over_30'
      ELSE 'No Age'
    END AS age_group_member,
    CASE
      WHEN eis_age_dim.age_member IN(
        '0-2', '3-5', '6-10', '11-15', '16-20', '21-25', '26-30', 'Billed_Under_31_Days'
      ) THEN '0-30 Days'
      WHEN eis_age_dim.age_member IN(
        '31-60', '61-90', '91-120', '121-150', '151-180'
      ) THEN eis_age_dim.age_alias
      WHEN eis_age_dim.age_member IN(
        '181-210', '211-240', '241-270', '271-300', '301-330', '331-360', 'Over_360'
      ) THEN '181+ Days'
      WHEN eis_age_dim.age_member IN(
        'Unbilled_31_Plus_Day'
      ) THEN 'Over 30 Days'
      ELSE 'No Age'
    END AS age_group_alias,
    eis_age_dim.age_gen02,
    eis_age_dim.age_gen02_alias,
    eis_age_dim.age_gen02_info,
    eis_age_dim.age_gen03,
    eis_age_dim.age_gen03_alias,
    eis_age_dim.age_gen03_info,
    eis_age_dim.age_gen04,
    eis_age_dim.age_gen04_alias,
    eis_age_dim.age_gen04_info,
    eis_age_dim.age_gen05,
    eis_age_dim.age_gen05_alias,
    eis_age_dim.age_gen06,
    eis_age_dim.age_gen06_alias,
    eis_age_dim.age_gen07,
    eis_age_dim.age_gen07_alias,
    eis_age_dim.age_gen08,
    eis_age_dim.age_gen08_alias,
    eis_age_dim.age_gen09,
    eis_age_dim.age_gen09_alias,
    eis_age_dim.age_gen10,
    eis_age_dim.age_gen10_alias,
    eis_age_dim.age_gen11,
    eis_age_dim.age_gen11_alias,
    eis_age_dim.age_gen12,
    eis_age_dim.age_gen12_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_age_dim
;
