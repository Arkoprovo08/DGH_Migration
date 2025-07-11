INSERT INTO upstream_data_management.t_measurement_of_petroleum_meter_details (
    measurement_of_petroleum_id,
    type_of_meter,
    make,
    size,
    range,
    percentage_error,
    is_internal_consumption_meter,
    is_flare_meter,
    is_custody_transfer_meter,
    is_active,
    created_by,
    creation_date,
    stand_by_meter_availability,
    is_migrated
)
SELECT
    hdr.measurement_of_petroleum_id,
    MAX(CASE WHEN m.label_text = 'Type of meter' THEN m.label_value END) AS type_of_meter,
    MAX(CASE WHEN m.label_text = 'Make' THEN m.label_value END) AS make,
    MAX(CASE WHEN m.label_text = 'Size' THEN m.label_value END) AS size,
    MAX(CASE WHEN m.label_text = 'Range' THEN m.label_value END) AS range,
    MAX(CASE WHEN m.label_text = 'Percentage error' THEN m.label_value END) AS percentage_error,
    0 AS is_internal_consumption_meter,
    1 AS is_flare_meter,
    0 AS is_custody_transfer_meter,
    1 AS is_active,
    mum.user_id AS created_by,
    MAX(m.created_on) AS creation_date,
    NULL::VARCHAR AS stand_by_meter_availability,
    1 AS is_migrated
FROM dgh_staging.form_measurement_petroleum m
JOIN (
    SELECT refid, num_seq
    FROM dgh_staging.form_measurement_petroleum
    WHERE label_text = 'Flare Meter'
      AND label_value = '1'
) flare 
  ON flare.refid = m.refid 
 AND m.num_seq BETWEEN flare.num_seq + 1 AND flare.num_seq + 5
JOIN upstream_data_management.t_measurement_of_petroleum_header hdr
    ON hdr.measurement_of_petroleum_application_number = m.refid
LEFT JOIN user_profile.m_user_master mum
    ON mum.migrated_user_id = m.created_by
WHERE mum.is_migrated = 1
GROUP BY hdr.measurement_of_petroleum_id, mum.user_id;
