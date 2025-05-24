# scale_test_prep.py

```
python scale_test_prep.py -h
usage: scale_test_prep.py [-h] --url URL --username USERNAME --password
                          PASSWORD --app-type APP_TYPE [--num-apps NUM_APPS]
                          [--event-instances-only]
```

This script is used to populate the database for scale testing purposes.
This populates the specified database with `--num-apps` rows into the `event_instances` table.
It looks for an example SummaryEvent in `event_instances` table for the appropriate `entity_type`
based on `--app-type` passed in (`hive` or `spark`), and makes clones of the example.
Each clone is created with a unique `id` (autogen), `event_instance_id`, `entity_id`.
`event_time`, `created_at`, and `updated_at` are set to the current timestamp.

On a test machine, bulk insert of 2 million records into `event_instances` happened in about 5 minutes.

For `--app-type hive`, `--num-apps` entries are made into `hive_queries` table.
This scans entries in `hive_queries` table whose row has `numMRJobs=2`, and uses that as the clone source
(2 was specifically chosen to handle multiple MR jobs but with a minimum number since the payload gets too large and
slows down inserts and blows up data storage requirements on the database with more MR Job data).
Clones have a unique `id` (autogen), and `query_id` set to the `entity_id` of the generated `event_instances` entries.
To save storage and data generation time, clones are not made for the corresponding entries in `jobs` table.  
Since the `annotation` field of `hive_queries` would have foreign keys into existing `jobs` entries, this still simulates
scale testing for `get_interesting_apps.py` well enough since it will still query for the corresponding `jobs` related to
generated `hive_queries` entries.

When running this, it is recommended to create a new partition in `event_instances` and `hive_queries` tables so that the
scale test data can be easily dropped.
