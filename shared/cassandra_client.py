from cassandra.cluster import Cluster
import os

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts, protocol_version=4, load_balancing_policy=None)
        self.session = self.cluster.connect()
        self.create_keyspace()
        self.init_schema("migrations_cs/cassandra_tables.cql")
        
    def create_keyspace(self):
        create_keyspace_query = """
        CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': '3'
        };
        """
        self.session.execute(create_keyspace_query)
        self.session.set_keyspace("sensor")

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query, parameters=None):
        try:
            print(f"Executing query: {query} with parameters: {parameters}")
            if parameters:
                return self.get_session().execute(query, parameters)
            else:
                return self.get_session().execute(query)
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Exception: {e}")

    def init_schema(self, schema_file_path):
        #print("FILE PATH ES ->>" ,os.path.abspath(schema_file_path))
        if not os.path.exists(schema_file_path):
            raise FileNotFoundError(f"Schema file not found: {schema_file_path}")
        try:
            with open(schema_file_path, 'r') as file:
                schema_queries = file.read()
                queries = [q.strip() for q in schema_queries.split(';') if q.strip()]
                for query in queries:
                    if query:
                        result = self.execute(query)
                        if result:
                            print("Query Result:", result.all())
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

