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
  MAX(CASE WHEN fmp.label_text = 'Type of meter' THEN fmp.label_value END) AS type_of_meter,
  MAX(CASE WHEN fmp.label_text = 'Make' THEN fmp.label_value END) AS make,
  MAX(CASE WHEN fmp.label_text = 'Size' THEN fmp.label_value END) AS size,
  MAX(CASE WHEN fmp.label_text = 'Range' THEN fmp.label_value END) AS range,
  MAX(CASE WHEN fmp.label_text = 'Percentage error' THEN fmp.label_value END) AS percentage_error,
  0 AS is_internal_consumption_meter,
  0 AS is_flare_meter,
  1 AS is_custody_transfer_meter,
  1 AS is_active,
  mum.user_id AS created_by,
  MAX(fmp.created_on) AS creation_date,
  CASE 
    WHEN standby.label_value = '1' THEN 'Yes'
    ELSE 'No'
  END AS stand_by_meter_availability,
  1 AS is_migrated
FROM dgh_staging.form_measurement_petroleum cust
JOIN dgh_staging.form_measurement_petroleum num_mtr
  ON num_mtr.refid = cust.refid
 AND num_mtr.num_seq = cust.num_seq + 1
 AND num_mtr.label_text = 'Number of meters'
JOIN dgh_staging.form_measurement_petroleum fmp
  ON fmp.refid = cust.refid
 AND fmp.num_seq BETWEEN cust.num_seq + 2 AND cust.num_seq + 1 + (CAST(num_mtr.label_value AS INTEGER) * 6)
 AND fmp.label_text IN ('Type of meter', 'Make', 'Size', 'Range', 'Percentage error')
JOIN upstream_data_management.t_measurement_of_petroleum_header hdr
  ON hdr.measurement_of_petroleum_application_number = cust.refid
LEFT JOIN user_profile.m_user_master mum
  ON mum.migrated_user_id = fmp.created_by AND mum.is_migrated = 1
LEFT JOIN LATERAL (
  SELECT label_value
  FROM dgh_staging.form_measurement_petroleum 
  WHERE refid = cust.refid 
    AND label_text = 'Stand by meter availability ( in case there is only one meter for custody transfer)'
  LIMIT 1
) standby ON true
WHERE cust.label_text = 'Custody Transfer Meter'
  AND cust.label_value = '1'
GROUP BY 
  hdr.measurement_of_petroleum_id,
  mum.user_id,
  standby.label_value,
  FLOOR((fmp.num_seq - (cust.num_seq + 2)) / 6);
