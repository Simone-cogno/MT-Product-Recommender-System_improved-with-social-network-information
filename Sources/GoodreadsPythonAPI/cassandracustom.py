__author__ = 'Simo'
from cassandra.cqlengine import connection
from cassandra.cluster import Cluster
from cassandra.cqlengine.query import DoesNotExist, MultipleObjectsReturned
import models
import uuid
from outils import *
import logging

#This class is used connect to the Cassandra cluster and perform some operation on it
class Cassandra:
    def __init__(self):
        self.session = None
        self.connect()
    #Connect to cassandra
    def connect(self):
        #connection.setup(['localhost'], "prs", protocol_version=3)
        connection.setup(['CASSANDRA_IP'], "prs", protocol_version=3)
        self.session = connection.get_session()

    def get_user_if_exsists_or_save(self, user):
        user_to_return = None
        try:
            q = models.Users.get(gid=user.gid)
        except DoesNotExist as e:
            user.save()
            user_to_return = user
        except MultipleObjectsReturned as e:
            raise e
        else:
            user_to_return = q
        finally:
            return user_to_return

    def save_shelve_if_not_exsists(self, shelve):
        object_to_return = None
        exsists = False
        try:
            q = models.Shelves.get(shelve=shelve.shelve)
        except DoesNotExist as e:
            shelve.save()
            object_to_return = shelve
        except MultipleObjectsReturned as e:
            logging.error(e.message)
            raise e
        else:
            exsists = True
            object_to_return = q
        finally:
            return (exsists, object_to_return)

    def save_if_not_exsists(self, object):
        object_to_return = None
        exsists = False
        try:
            q = object.__class__.get(gid=object.gid)
        except DoesNotExist as e:
            object.save()
            object_to_return = object
        except MultipleObjectsReturned as e:
            logging.error(e.message)
            raise e
        else:
            exsists = True
            object_to_return = q
        finally:
            return (exsists, object_to_return)

    # Save books and reviews to cassandra
    def saveUserReviews(self, user, reviewDict):
        logging.info( 'Save user reviews:')
        logging.info( '\t-saving %d books %d authors %d reviews', len(reviewDict['books']), len(reviewDict['authors']), len(reviewDict['reviews']))

        for i, ((book, author), review) in enumerate(
                zip(zip(reviewDict['books'], reviewDict['authors']), reviewDict['reviews'])):
            if(i%100==0):
                logging.info("%d reviews saved", i)
            exsists, bookdb = self.save_if_not_exsists(book)
            exsists, authordb = self.save_if_not_exsists(author)
            if (bookdb.author.id is None):
                bookdb.author = authors_to_author(authordb)
                bookdb.save()
            exsists, reviewdb = self.save_if_not_exsists(review)
            logging.debug("%r %d %s", exsists, review.gid, reviewdb.id)
            if (exsists is False):
                reviewdb.book = books_to_book(bookdb)
                reviewdb.save()
                if (user.list_reviews is not None):
                    already_on_list = False
                    for review_user in user.list_reviews:
                        if (review_user.id == reviewdb.id):
                            already_on_list = True
                            break
                    if(already_on_list is False):
                        user.list_reviews.append(reviews_to_review(reviewdb))

                else:
                    user.list_reviews.append(reviews_to_review(reviewdb))
        user.reviews_retrieved=True
        user.private=False
        user.save()

    #Save a list of users
    def saveListUsers(self, listUSers):
        for user in listUSers:
            self.save_if_not_exsists(user)

    def saveListUsersAndAppendToListFriends(self, user, listFriends):
        for friend in listFriends:
            exsists, savedUser = self.save_if_not_exsists(friend)
            saved_user_type = models.user(id=savedUser.id, gid=savedUser.gid,
                                          name=savedUser.name, friends_count=savedUser.friends_count,
                                          small_user=savedUser.small_user, private=savedUser.private)
            user.list_friends.append(saved_user_type)
        user.friends_retrieved = True
        user.save()

    def getSession(self):
        return self.session

    #update the books entry by adding shelves and other information not privded early
    def updateSmallBook(self, book, id):
        logging.debug("update small book %d id: %s", book.gid, id)
        bookdb=models.Books.get(id=id)
        bookdb.title=book.title
        bookdb.isbn=book.isbn
        bookdb.isbn13=book.isbn13
        bookdb.publication_date=book.publication_date
        bookdb.description=book.description
        bookdb.average_rating=book.average_rating
        bookdb.ratings_count=book.ratings_count
        bookdb.reviews_count=book.reviews_count
        bookdb.small_book=False
        #Specific to mode   #Specific to mode
        bookdb.shelves_retrieved=True
        #Lists   #Lists
        bookdb.list_shelves=book.list_shelves
        bookdb.save()
        logging.info("Saved %d shelves",len(book.list_shelves))
        #self.addBooktoShelve(bookdb, book.list_shelves)


    #Add a book to the shelve table
    def addBooktoShelve(self,book, shelves):
        for shelve in shelves:
            exsists, shelvedb=self.save_shelve_if_not_exsists(shelve_to_shelves(shelve))
            if (shelvedb.list_book is not None):
                already_on_list = False
                for shelve_book in shelvedb.list_book:
                    if (shelve_book.id == book.id):
                        already_on_list = True
                        break
                if (already_on_list is False):
                    shelvedb.list_book.append(books_to_book(book))
                    shelvedb.save()
            else:
                shelvedb.list_book.append(books_to_book(book))
                shelvedb.save()



