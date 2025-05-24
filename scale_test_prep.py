import argparse
from datetime import datetime
import json
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from get_interesting_apps import DB
from sqlalchemy.sql import text
from tqdm import tqdm

class ScaleTestPrep(object):

    entity_type_map = {
        'hive': 1,
        'spark': 2
    }

    def __init__(self, url, username, password, app_type):
        self.sql_engine = DB(url, username, password).sql_engine
        self.app_type = app_type
        self.entity_type = self.entity_type_map[app_type]

    def populate_app_data(self, num_apps, event_instances_only):
        entity_ids = self.populate_event_instances(num_apps)
        print('{} {} event_instances records created'.format(len(entity_ids), self.app_type))
        if not event_instances_only:
            if self.app_type == 'hive':
                self.populate_hive_data(num_apps, entity_ids)
            elif self.app_type == 'spark':
                self.populate_spark_data(num_apps, entity_ids)
            else:
                raise ValueError('Unsupported app_type {}'.format(self.app_type))
        else:
            print('Skipping adding entries to {} specific tables'.format(self.app_type))

    def populate_event_instances(self, num_apps):
        rows = self.execute("select max(event_instance_id) from event_instances")
        max_event_instance_id = rows[0][0]
        rows = self.execute("select * from event_instances where event_type='SU' and entity_type={} limit 1".format(self.entity_type))
        row = rows[0]
        column_names = ','.join([col for col in row.keys() if col != 'id'])
        column_placeholders = ','.join([':{}'.format(col) for col in row.keys() if col != 'id'])
        insert_statement = text("""
            insert into event_instances ({}) values
            ({})
            """.format(column_names, column_placeholders))

        orig_d = dict(row)
        del orig_d['id']

        batch_size = 1000
        num_batches = num_apps / batch_size

        entity_ids = []

        with self.sql_engine.connect() as conn:
            trans = conn.begin()
            for batch in tqdm(range(num_batches)):
                D = []
                for i in range(batch_size):
                    d = orig_d.copy()
                    d['event_instance_id'] = max_event_instance_id + batch * batch_size + i
                    d['entity_id'] = '_test_{}_{}'.format(self.app_type, batch * batch_size + i)
                    entity_ids.append(d['entity_id'])
                    d['comment'] = '_test'
                    now = datetime.now()
                    d['event_time'] = now
                    d['created_at'] = now
                    d['updated_at'] = now
                    D.append(d)
                conn.execution_options(autocommit=False).execute(insert_statement, D)
            trans.commit()

        return entity_ids

    def populate_hive_data(self, num_apps, entity_ids):
        # create hive_queries
        rows = self.execute("select * from hive_queries limit 100")
        num_mr_jobs = 0
        for row in rows:
            annotation = json.loads(row['annotation'])
            if annotation['numMRJobs'] == 2:
                num_mr_jobs = annotation['numMRJobs']
                break

        if num_mr_jobs > 0:
            print('Found a hive query with {} mr jobs'.format(num_mr_jobs))
        else:
            print('Did not find a hive query with any mr jobs')

        column_names = ','.join([col for col in row.keys() if col != 'id'])
        column_placeholders = ','.join([':{}'.format(col) for col in row.keys() if col != 'id'])
        insert_statement = text("""
            insert into hive_queries ({}) values
            ({})
            """.format(column_names, column_placeholders))

        orig_d = dict(row)
        del orig_d['id']

        batch_size = 10
        num_batches = num_apps / batch_size

        with self.sql_engine.connect() as conn:
            trans = conn.begin()
            for batch in tqdm(range(num_batches)):
                D = []
                for i in range(batch_size):
                    d = orig_d.copy()
                    d['query_id'] = entity_ids[batch * batch_size + i]
                    now = datetime.now()
                    d['created_at'] = now
                    d['updated_at'] = now
                    D.append(d)
                conn.execution_options(autocommit=False).execute(insert_statement, D)
            trans.commit()
        # no need to create `jobs` entries, since the hive_queries.annotation will point to existing MR jobs

    def populate_spark_data(self, num_apps, entity_ids):
        # TODO
        # create blackboards
        pass

    def execute(self, query):
        with self.sql_engine.connect() as conn:
            result = conn.execute(query)
            return result.fetchall()

    def stat(self):
        rows = self.execute("select count(*) from event_instances where event_type='{}' and entity_type='{}'".format('SU', self.entity_type))
        print('Found {} {} apps with recommendations'.format(rows[0][0], self.app_type))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--app-type', required=True)
    parser.add_argument('--num-apps', type=int, default=0)
    parser.add_argument('--event-instances-only', action='store_true')

    args = parser.parse_args()

    prep = ScaleTestPrep(args.url, args.username, args.password, args.app_type)
    prep.stat()
    if args.num_apps > 0:
        prep.populate_app_data(args.num_apps, args.event_instances_only)
    else:
        print('To add new entries, set --num-apps to a positive number')


