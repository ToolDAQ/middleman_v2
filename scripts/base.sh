#!/bin/bash
echo "Inserting a default user"
psql -ddaq -c "INSERT INTO users (username, password_hash) VALUES ('dev_user', 'c20cc404fe15337ce6d8a5b782576d9a21de03f8707065c8ccf7abb1cc939801');"

echo "inserting a dummy device"
psql -ddaq -c "INSERT INTO devices ( name, description, author_id ) VALUES ( 'demo_device', 'test device', 1 );"

echo "inserting middleman device"
psql -ddaq -c "INSERT INTO devices ( name, description, author_id ) VALUES ( 'middleman', 'interface application to online database', 1 );"

echo "Inserting example monitoring data"
psql -ddaq -c "INSERT INTO monitoring (time, device_id, subject, data) SELECT now() - (i * INTERVAL '1 minute') AS time, 1 as device_id, 'general' AS subject, json_build_object( 'temperature', round((random() * 50 + 10)::numeric, 2), 'humidity', round((random() * 100)::numeric, 2)) AS data FROM generate_series(1, 100) i;"

echo "Inserting example plotyplot data"
psql -ddaq -c "INSERT INTO plotlyplots (name, time, version, data, layout) VALUES ('plotly1', 'now()', 1, '[{\"x\": [1, 2, 3, 4, 5], \"y\": [10, 20, 15, 30, 25], \"type\": \"scatter\"}]', '{\"title\": \"plotly1\"}');"

echo "Inserting dummy runmode configuration"
psql -ddaq -c "INSERT INTO runmode_config ( name, description, author_id, data ) VALUES ( 'dummy', 'empty runmode', 1, '{}' );"

