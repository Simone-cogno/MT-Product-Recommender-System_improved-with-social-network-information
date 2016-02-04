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



#Get friends of a user
def getFriends(c, g,  user, lvl=1, maxLevel=5, count=0, totalFriends=0):
    s=space(lvl)
    if(lvl>maxLevel):
        return
    friends=None
    user=c.get_user_if_exsists_or_save(user)
    if(user is None):
        return
    logging.info(s+"Get friends for user %d id: %s on level %d (%d of %d): ", user.gid, user.id, lvl, count, user.friends_count)
    if(user.friends_retrieved):
        logging.warning(s+'-->Friends of %d id: %s already retrieved',user.gid,user.id),
        friends_count=len(user.list_friends)
        logging.info(s+'--> has %d friends on database of %d on goodreads',friends_count, user.friends_count),
        if(friends_count!=user.friends_count):
            logging.error(s+'user %d id: %s has %d friends on database of %d on goodreads',user.gid, user.id, friends_count, user.friends_count)
        friends=user.list_friends
    else:
        friends, total=g.getFriends(user, s)
        if(friends is None): #User is private or not accessible
            logging.warning(s+"-->user %d id: %s is private",user.gid, user.id)
            user.private=True
            user.save()
            return
        else:
            if(len(friends)!=total):
                logging.error(s+'user %d id: %s has %d friends on database of %d on goodreads',user.gid, user.id, len(friends), total)
            #Save friends
            logging.info(s+'-->Finish getting friends of %s, saving %d users on level %d...',user.id,len(friends), lvl),
            c.saveListUsersAndAppendToListFriends(user,friends)
            logging.info(s+'finish')
    #logging.info( that max level reached
    if(lvl+1>maxLevel):
            logging.info(s+'--> Max level')
    #Retrieve friends of friends
    count=0
    for friend in friends:
        count+=1
        getFriends(c, g, friend, lvl+1, maxLevel, count, user.friends_count)
    logging.info(s+'-->Finish getting friends of %s (level %d)',user.id, lvl)

#Get users reviews
def getUsersReviews(c,g):
    for user in Users.objects().timeout(10).filter(reviews_retrieved=False):
        if(user['reviews_retrieved'] is False):
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
                 c.saveUserReviews(user,reviewsDict)
                 logging.info("%d reviews saved for user %d id: %s user reviews retrieved set to %r", len(reviewsDict['reviews']), user.gid, user.id, user.reviews_retrieved)
        else:
             logging.warning("Reviews for user %d id: %s already retrieved", user.gid, user.id)
#Set books with
def processBook(c,g):

    rows = c.getSession().execute('SELECT id, small_book FROM prs.books')
    for row in rows:
        if(row['small_book'] is None):
            c.getSession().execute('UPDATE prs.books SET small_book = true WHERE id=%s', (row['id'], ))

def getBookDetails(c,g):
    rows = c.getSession().execute('SELECT id,gid FROM prs.books WHERE shelves_retrieved=false')
    for row in rows:
        logging.info("Retrieving shelves for %s %s", row['id'], row['gid'])
        book, similarBook=g.getBook(int(row['gid']))
        if(book is None):
            b=Books.get(gid=int(row['gid']))
            b.error_retrieving_shelves=True
            b.save()
            continue
        else:
            logging.info("books %s %s retrieved", row['id'], row['gid'])
            c.updateSmallBook(book, row['id'])
            logging.info("Finish saving %s %s", row['id'], row['gid'])




#Sync the Cassandra model
def syncTables():
    sync_table(Books)
    sync_table(Shelves)
    sync_table(Authors)
    sync_table(Users)
    sync_table(Reviews)

#Initialize the Cassandra connection and the Goodreads API connection
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
    getFriends(c,g,u, maxLevel=4)


