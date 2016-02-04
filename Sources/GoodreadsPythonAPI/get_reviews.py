__author__ = 'Simo'
from models import *
from goodreads import Goodreads
from cassandracustom import Cassandra
from cassandra.cqlengine.management import sync_table, sync_type
from cassandra.cqlengine.query import DoesNotExist, MultipleObjectsReturned
from outils import space, initialize_logger
from cassandra.query import SimpleStatement
import datetime
from uuid import uuid4, UUID
from types import ListType
import time
import logging
import os.path
import coloredlogs
import time


#Get reviews for all the users in the Users table
def getUsersReviews(c,g):
    while(True):
        for row in c.getSession().execute("SELECT id,gid FROM prs.users WHERE reviews_retrieved=false "):
            user=Users.get(id=row['id'])
            if(user.private):
                logging.warning("USer  %d id: %s is private", user.gid, user.id)
                continue
            if(user.reviews_retrieved is False):
                 user.reviews_retrieved=True
                 user.save()
                 reviewsDict, already_retrieved=g.getUserReviews(user)
                 if(reviewsDict is None):
                     if(already_retrieved is False):
                         logging.warning('user %d id:%s is private for retrieveing reviews', user.gid, user.id)
                         user.private=True
                         user.reviews_retrieved=False
                         user.save()
                     else:
                         logging.warning("Reviews for user %d id: %s already retrieved", user.gid, user.id)
                 else:
                     if reviewsDict['reviews'] is None or len(reviewsDict['reviews'])==0:
                         user.private=True
                         user.reviews_retrieved=False
                         user.save()
                         logging.warning('No rewiews for user %d id:%s', user.gid, user.id)
                     else:
                         c.saveUserReviews(user,reviewsDict)
                         logging.info("%d reviews saved for user %d id: %s user reviews retrieved set to %r", len(reviewsDict['reviews']), user.gid, user.id, user.reviews_retrieved)
            else:
                logging.warning("Reviews for user %d id: %s already retrieved", user.gid, user.id)
        time.sleep(1)


#Sync the model to Cassandra
def syncTables():
    sync_table(Books)
    sync_table(Shelves)
    sync_table(Authors)
    sync_table(Users)
    sync_table(Reviews)

#Initialize Cassandra and Goodreads API
def init():
    initialize_logger(os.getcwd())
    coloredlogs.install(level='DEBUG')
    coloredlogs.ColoredFormatter()
    c=Cassandra()
    syncTables()
    g=Goodreads()
    return (c,g)

if __name__ == "__main__":
    c,g=init()
    u=c.get_user_if_exsists_or_save(Users(id=uuid.uuid4(), gid=47225465,
                                          name='Simone', friends_count=1,
                                          small_user=True))
    getUsersReviews(c,g)


