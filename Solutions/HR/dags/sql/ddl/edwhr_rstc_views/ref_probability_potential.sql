/*==============================================================*/
/* Table: Ref_Probability_Potential                             */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_probability_potential AS SELECT
    ref_probability_potential.probability_potential_id,
    ref_probability_potential.probability_potential_desc,
    ref_probability_potential.source_system_code,
    ref_probability_potential.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_probability_potential
;
