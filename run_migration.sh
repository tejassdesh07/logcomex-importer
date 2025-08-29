#!/bin/bash

# Script to run the broker fields migration
# Make sure your MySQL container is running

echo "Running broker fields migration..."

# Run the migration using Docker and MySQL client
docker run --rm \
  --network host \
  -v $(pwd):/migration \
  mysql:8.0 \
  mysql -h 127.0.0.1 -P 3306 \
  -u sarvesh \
  -pSaved6-Hydrogen-Smirk-Paltry-Trimmer \
  logcomex \
  < /migration/add_broker_fields.sql

echo "Migration completed!"
echo "You can now run your import endpoint to test the new broker percentage fields."
