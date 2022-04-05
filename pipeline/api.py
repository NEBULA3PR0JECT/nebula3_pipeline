import random
import sys
import time
from nebula3_database.movie_db import MOVIE_DB
from nebula3_database.database.arangodb import DatabaseConnector
from nebula3_database.config import NEBULA_CONF


class PIPELINE_API:
    def __init__(self):
        config = NEBULA_CONF()
        self.database = config.get_database_name()
        self.dbconn = DatabaseConnector()
        self.db = self.dbconn.connect_db(self.database)
        self.movie_db = MOVIE_DB(self.db)

    def get_new_movies(self):
        return self.movie_db.get_new_movies()

    def get_all_movies(self):
        return self.movie_db.get_all_movies()

    # def get_plugins(self):
    #     self.experts = []
    #     query = 'FOR doc IN nebula_experts RETURN doc'
    #     cursor = self.db.aql.execute(query)
    #     for data in cursor:
    #         #print(data)
    #         self.experts.append(data)

    # def register_plugin(self, port, _module, klass, filter):
    #     self.filter = filter
    #     self.port = port
    #     self._module = _module
    #     self.klass = klass

    def get_versions(self):
        versions = []
        query = 'FOR doc IN changes RETURN doc'
        cursor = self.db.aql.execute(query)
        for data in cursor:
            #print(data)
            versions.append(data)
        return(versions)

    def get_expert_status(self, expert, depends):
        versions = self.get_versions()
        for version in versions:
            if version[depends] > version[expert]:
                #print(version)
                return True
            else:
                return False

    def wait_for_change(self, expert, depends):
        while True:
            if self.get_expert_status(expert, depends):
                movies = self.get_new_movies()
                #print("New movies: ", movies)
                return(movies)
            time.sleep(3)

    def wait_for_finish(self, experts):
        while True:
            versions = self.get_versions()
            count = len(experts)
            for version in versions:
                global_version = version['movies']
                print(version)
                for expert  in experts:
                    if global_version != version[expert]:
                        break
                    else:
                        count = count - 1
            if count <= 0:
                return True
            time.sleep(3)

    def update_expert_status(self, expert):
        if expert == "movies": #global version
            txn_db = self.db.begin_transaction(read="changes", write="changes")
            print("Updating global version")
            query = 'FOR doc IN changes UPDATE doc WITH {movies: doc.movies + 1} in changes'
            txn_db.aql.execute(query)
            txn_db.transaction_status()
            txn_db.commit_transaction()
            return True
        else:
            txn_db = self.db.begin_transaction(read="changes", write="changes")
            query = 'FOR doc IN changes UPDATE doc WITH {' + expert + ': doc.movies} in changes'
            #print(query)
            txn_db.aql.execute(query)
            txn_db.transaction_status()
            txn_db.commit_transaction()
            return True

    # def force_start_expert(self, expert):
    #     print("Updating global version")
    #     query = 'FOR doc IN changes UPDATE doc WITH {' + expert + ': doc.'+ expert + ' - 1} in changes'
    #     #print(query)
    #     self.db.aql.execute(query)

    # def change_status_movie(self, status, movie_id):
    #     query = 'FOR doc IN Movies FILTER doc._id == \'' + movie_id + '\' UPDATE doc WITH {status: \''+ status +'\' } in Movies'
    #     self.db.aql.execute(query)

    # def get_all_expert_data(self, expert, movie_id):
    #     #Actions, Actors
    #     expert_data = []
    #     query = 'FOR doc IN Nodes FILTER doc.arango_id == \'' + movie_id + '\' AND doc.class == \'' + expert + '\'AND (HAS(doc,"bboxes") OR HAS(doc,"box")) RETURN doc'
    #     cursor = self.db.aql.execute(query)
    #     for node in cursor:
    #         expert_data.append(node)
    #     return(expert_data)

## for testing uncomment next line
#__name__ = 'test'
def test():
    pipeline = PIPELINE_API()
    movies = pipeline.get_new_movies()
    print(movies)
    versions = pipeline.get_versions()
    print (versions)

if __name__ == 'test':
    test()
