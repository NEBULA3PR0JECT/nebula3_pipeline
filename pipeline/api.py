import os
import sys
import time
import threading
import logging
# add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from nebula3_database.movie_db import MOVIE_DB
from nebula3_database.database.arangodb import DatabaseConnector
from nebula3_database.config import NEBULA_CONF


class PipelineApi:
    def __init__(self, logger: logging.Logger):
        config = NEBULA_CONF()
        self.running = True
        self.logger = logger
        self.subscriptions = list()
        self.database = config.get_database_name()
        self.dbconn = DatabaseConnector()
        self.db = self.dbconn.connect_db(self.database)
        self.movie_db = MOVIE_DB(self.db)

    def __del__(self):
        self.running = False
        for sub_thread in self.subscriptions:
            if (sub_thread.is_alive()):
                sub_thread.join()

    def subscription_loop(self, entity_name: str, entity_dependency: str, msg_cb):
        while self.running:
            # Signaling your code, that we have newly uploaded movie, frames are stored in S3.
            # Returns movie_id in form: Movie/<xxxxx>
            movies = self.wait_for_change(entity_name, entity_dependency)
            for movie in movies:
                msg_cb(movie)
            self.update_expert_status(entity_name) #Update scheduler, set it to done status

    def subscribe(self, entity_name: str, entity_dependency: str, msg_cb):
        """subscribe for msgs

        Args:
            entity_name (str): the pipeline entity name
            entity_dependency (str): the pipeline entity dependency
            msg_cb (_type_): _description_
        """
        sub_thread = threading.Thread(target=self.event_loop,
                                             args=[entity_name, entity_dependency, msg_cb])
        sub_thread.start()
        self.subscriptions.append(sub_thread)

    def get_new_movies(self):
        return self.movie_db.get_new_movies()

    def get_all_movies(self):
        return self.movie_db.get_all_movies()

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


## for testing uncomment next line
#__name__ = 'test'
def test():
    pipeline = PipelineApi()
    movies = pipeline.get_new_movies()
    print(movies)
    versions = pipeline.get_versions()
    print (versions)

if __name__ == 'test':
    test()
