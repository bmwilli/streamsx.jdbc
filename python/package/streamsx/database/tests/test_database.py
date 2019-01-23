from unittest import TestCase

import streamsx.database as db

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr

import datetime
import os
import json

##
## Test assumptions
##
## Streaming analytics service running
## DB2 Warehouse service credentials are located in a file referenced by environment variable DB2_CREDENTIALS
##

class TestDB(TestCase):

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)

    def test_string_type(self):
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology('test_String_input')

        sql_create = 'CREATE TABLE STR_SAMPLE (A CHAR(10), B CHAR(10))'
        sql_insert = 'INSERT INTO STR_SAMPLE (A, B) VALUES (\'hello\', \'world\')'
        sql_drop = 'DROP TABLE STR_SAMPLE'
        s = topo.source([sql_create, sql_insert, sql_drop]).as_string()
        res_sql = db.run_statement(s, credentials)
        res_sql.print()
        tester = Tester(topo)
        tester.tuple_count(res_sql, 3)
        #tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config)


    def test_mixed_types(self):
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()

        topo = Topology('test_SPLBeacon_Functor_JDBC')
        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':1})
        pulse.A = pulse.output('"hello"')
        pulse.B = pulse.output('"world"')

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')
        query_schema = StreamSchema('tuple<rstring sql>')

        sql_create = 'CREATE TABLE RUN_SAMPLE (A CHAR(10), B CHAR(10))'
        create_table = db.run_statement(pulse.stream, credentials, schema=sample_schema, sql=sql_create)
 
        sql_insert = 'INSERT INTO RUN_SAMPLE (A, B) VALUES (?, ?)'
        inserts = db.run_statement(create_table, credentials, schema=sample_schema, sql=sql_insert, sql_params="A, B")

        query = op.Map('spl.relational::Functor', inserts, schema=query_schema)
        query.sql = query.output('"SELECT A, B FROM RUN_SAMPLE"')

        res_sql = db.run_statement(query.stream, credentials, schema=sample_schema, sql_attribute='sql')
        res_sql.print()

        sql_drop = 'DROP TABLE RUN_SAMPLE'
        drop_table = db.run_statement(res_sql, credentials, sql=sql_drop)

        tester = Tester(topo)
        tester.tuple_count(drop_table, 1)
        #tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config)

