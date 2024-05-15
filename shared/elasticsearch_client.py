from elasticsearch import Elasticsearch
import time

class ElasticsearchClient:
    def __init__(self, host="localhost", port="9200"):
        self.host = host
        self.port = port
        self.client = Elasticsearch(["http://"+self.host+":"+self.port])

        while not self.ping():
            print("Waiting for Elasticsearch to start...")
            time.sleep(1)
        self.setup_index_and_mapping()
        

    def ping(self):
        return self.client.ping()
    
    def clearIndex(self, index_name):
        if self.client.indices.exists(index=index_name):
            # If the index exists, delete it
            return self.client.indices.delete(index=index_name)
        else:
            # If the index does not exist, do nothing
            return None
    
    def close(self):
        self.client.close()

    def create_index(self, index_name):
        # If the index does not exists, create it
        if not self.client.indices.exists(index=index_name):
            return self.client.indices.create(index=index_name)
        else:
            return None
    
    def create_mapping(self, index_name, mapping):
        return self.client.indices.put_mapping(index=index_name, body=mapping)
    
    def search(self, index_name, query):
        return self.client.search(index=index_name, body=query)
    
    def index_document(self, index_name, document):
        return self.client.index(index=index_name, body=document)
    
    
    def setup_index_and_mapping(self):
        index_name = "sensors"
        mapping = {
            "properties": {
                "name": {"type": "text",
                     "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                     }    
                },
                "description": {"type": "text"},
                "type": {"type": "text"},
            }
        }
        self.create_index(index_name)
        self.create_mapping(index_name, mapping)

    

    
    
    
