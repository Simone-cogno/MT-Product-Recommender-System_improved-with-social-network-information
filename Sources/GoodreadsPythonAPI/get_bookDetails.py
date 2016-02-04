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
import random

#Get more details of the books (list of shelves, and general details)
def getBookDetails(c, g):
    while (True):
        rows = c.getSession().execute('SELECT id,gid FROM prs.books WHERE shelves_retrieved=false')
        skip = 0
        for row in rows:
            if (skip > 0):
                skip -= 1
                logging.warning("Skipping %d", skip)
                continue
            b = c.getSession().execute('SELECT shelves_retrieved FROM prs.books WHERE id=%s', (row['id'],))
            if b[0]['shelves_retrieved'] == True:
                logging.warning("Shelve already retrieved for book %s %s", row['id'], row['gid'])
                skip = int(random.random()*20)
                continue
            else:
                logging.info("Retrieving shelves for %s %s", row['id'], row['gid'])
                book, similarBook = g.getBook(int(row['gid']))
                if (book is None):
                    b = Books.get(gid=int(row['gid']))
                    b.error_retrieving_shelves = True
                    b.save()
                    continue
                else:
                    b = c.getSession().execute('SELECT shelves_retrieved FROM prs.books WHERE id=%s', (row['id'],))
                    if (b[0]['shelves_retrieved'] == True):
                        logging.warning("Shelve already retrieved for book %s %s", row['id'], row['gid'])
                        skip = int(random.random()*20)
                        continue
                    logging.info("books %s %s retrieved", row['id'], row['gid'])
                    c.updateSmallBook(book, row['id'])
                    logging.info("Finish saving %s %s", row['id'], row['gid'])
        time.sleep(5)

#Sync the model to Cassandra
def syncTables():
    sync_table(Books)
    sync_table(Shelves)
    sync_table(Authors)
    sync_table(Users)
    sync_table(Reviews)

#Init cassandra and goodreads api
def init():
    initialize_logger(os.getcwd())
    coloredlogs.install(level='DEBUG')
    coloredlogs.ColoredFormatter()
    c = Cassandra()
    syncTables()
    g = Goodreads()
    return (c, g)


if __name__ == "__main__":
    c, g = init()
    u = c.get_user_if_exsists_or_save(Users(id=uuid.uuid4(), gid=47225465,
                                            name='Simone', friends_count=1,
                                            small_user=True))
    getBookDetails(c, g)
