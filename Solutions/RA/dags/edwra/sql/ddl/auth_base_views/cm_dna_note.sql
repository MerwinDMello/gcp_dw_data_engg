CREATE OR REPLACE VIEW {{ params.param_auth_base_views_dataset_name }}.cm_dna_note AS
SELECT
    cm_dna_note.note_id,
    cm_dna_note.eff_from_date_time,
    cm_dna_note.claim_id,
    cm_dna_note.note_type_id,
    cm_dna_note.note_text,
    cm_dna_note.note_data_text,
    cm_dna_note.user_id,
    cm_dna_note.source_created_date_time_utc,
    cm_dna_note.source_updated_date_time_utc,
    cm_dna_note.eff_to_date_time,
    cm_dna_note.source_system_code,
    cm_dna_note.dw_last_update_date_time
FROM
{{ params.param_cm_project_id }}.auth_base_views.cm_dna_note