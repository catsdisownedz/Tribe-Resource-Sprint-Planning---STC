-- sql/seed_sample.sql
-- Set current quarter and minimal sample temp rows (your example).
INSERT INTO quarters(label, is_current) VALUES ('2025Q3', TRUE)
ON CONFLICT (label) DO UPDATE SET is_current = EXCLUDED.is_current;

INSERT INTO tribes(name) VALUES
  ('Digital Tribe'), ('Business Operations'), ('Fixed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO apps(name) VALUES
  ('B2CCRM/ULA'), ('CRM/RBM/BSSTEAI')
ON CONFLICT (name) DO NOTHING;

WITH q AS (SELECT id FROM quarters WHERE is_current = TRUE)
INSERT INTO temp_assignments(quarter_id, tribe_id, app_id, resource_name, role, assign_type)
VALUES
((SELECT id FROM q), (SELECT id FROM tribes WHERE name='Digital Tribe'),       (SELECT id FROM apps WHERE name='B2CCRM/ULA'),      'Mohanad Bin Taleb',    'Designer',  'Dedicated'),
((SELECT id FROM q), (SELECT id FROM tribes WHERE name='Business Operations'), (SELECT id FROM apps WHERE name='B2CCRM/ULA'),      'Siri Chandana Vemana', 'Developer', 'Dedicated'),
((SELECT id FROM q), (SELECT id FROM tribes WHERE name='Fixed'),               (SELECT id FROM apps WHERE name='CRM/RBM/BSSTEAI'), 'Abhishekha Behera',    'Tester',    'Shared')
ON CONFLICT DO NOTHING;
