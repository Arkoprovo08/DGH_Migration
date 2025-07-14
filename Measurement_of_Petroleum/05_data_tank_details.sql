INSERT INTO upstream_data_management.t_measurement_of_petroleum_tank_details (
  measurement_of_petroleum_id,
  type_of_tank,
  capacity,
  is_active,
  created_by,
  creation_date,
  is_migrated
)
SELECT
  hdr.measurement_of_petroleum_id,
  MAX(CASE 
        WHEN fmp.label_text = 'Type of Tank' AND fmp.label_value = '1' THEN 'Fixed Roof'
        WHEN fmp.label_text = 'Type of Tank' AND fmp.label_value = '2' THEN 'Floating Roof'
        ELSE NULL
      END) AS type_of_tank,
  MAX(CASE WHEN fmp.label_text = 'Capacity' THEN fmp.label_value END) AS capacity,
  1 AS is_active,
  mum.user_id AS created_by,
  MAX(fmp.created_on) AS creation_date,
  1 AS is_migrated
FROM dgh_staging.form_measurement_petroleum fmp
JOIN upstream_data_management.t_measurement_of_petroleum_header hdr
  ON hdr.measurement_of_petroleum_application_number = fmp.refid
LEFT JOIN user_profile.m_user_master mum
  ON mum.migrated_user_id = fmp.created_by
WHERE fmp.label_text IN ('Type of Tank', 'Capacity')
  AND fmp.status = '1'
  AND mum.is_migrated = 1
GROUP BY 
  hdr.measurement_of_petroleum_id,
  mum.user_id,
  FLOOR((
    fmp.num_seq - (
      SELECT MIN(num_seq)
      FROM dgh_staging.form_measurement_petroleum nt
      WHERE nt.refid = fmp.refid 
        AND nt.label_text = 'Number of Tanks'
        AND nt.status = '1'
    )
  ) / 5)
ORDER BY 1;
