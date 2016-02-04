__author__ = 'Simo'
from goodreads import Goodreads
from cassandracustom import Cassandra
from cassandra.cqlengine.management import sync_table
import datetime
from uuid import uuid4, UUID
from types import ListType
import time
import logging
import os.path
import coloredlogs
from outils import space, initialize_logger, ascii,books_to_book
import models


#Test the cassandra driver
def testModel():
    book = {'gid': 12345,
         'title':"Weila",
         'isbn':"123410234",
         'isbn13':"212341124",
         'publication_date':datetime.datetime.now(),
         'description':"",
         'average_rating':0.5,
         'ratings_count':5,
         'reviews_count':10,
         'shelves':"Fantasy, Drama, Sci-Fi"}
    b=models.Books.create(**book)
    b.save()

    user=models.Users(gid=123456, name="Simone")
    user.save()

#Test the authentication at the goodreads api
def test_auth():
    g=Cassandra()
    g.auth()

#test of retrieveing a book from the G. API
def testBook():
    g=Goodreads()
    book, similar_books=g.getBook('50')
    print book.id, book.gid, book.author, book.list_shelves
    for shelve in book.list_shelves:
        print shelve.shelve, shelve.count, shelve.gid
    return
    #book.save()
    #Shelves
    #Similar books
    #for b in similar_books:
    #    b['book'].save()
    #    saveAuthors(b['authors'], b['book'])

    #print book.id
    #q=Shelves.objects.filter(Shelves.book_id==UUID('de90c445-2dcd-4945-ab73-6e7bd7a396c6'))
    #print q
    #for s in q:
    #    print s
#test of retrieveing the books reviews from the G. API
def testBookReviews():
    Goodreads().getBookReviews('0142437174')

#test of retrieveing the user reviews from the G. API
def testUserReviews(c):
    #user=Users(id=uuid4(), gid='902976', name='Katie') #902976 katie, 1085121 Stephanie, 47225465 mio
    user=models.Users(id=uuid.uuid4(), gid=1085121, name='Simone', friends_count=1,small_user=True)
    u=c.get_user_if_exsists_or_save(user)
    reviewsDict, altready_retrieved=Goodreads().getUserReviews(u)
    if(altready_retrieved is False):
        c.saveUserReviews(u,reviewsDict)
    else:
        print "already retrieved"

#test saving an author to Cassandra
def saveAuthors(authors, book):
    if isinstance(authors, ListType):
        for a in authors:
            a.save()
            models.AuthorBook.create(author=a.id, book=book.id)
    else:
        authors.save()
        models.AuthorBook.create(author=authors.id, book=book.id)

#Test creating and saving auser to Cassandra
def testUser():
    Cassandra()
    sync_table(models.Users)
    g=Goodreads()
    u=g.getUser('4134243')
    u.save()

#test retrieving user friends
def testUserFriends():
    c=Cassandra()
    g=Goodreads(new=False)
    user=models.Users(id=uuid4(), gid='3371638', name='Katie')
    friends, friendRelation, total=g.getFriends(user)
    if(friends is None):
        print "Private"
    else:
        print len(friends), len(friendRelation), total

#test an hash function of a string
def hashString(s):
    h= 7
    for i in range(0,len(s)):
        h = h*1223
        h=h+(ord(s[i]))
    return h%4222234741

#test the reliability of the hashfunction
def testHashFunction(c):
    rows = c.getSession().execute('SELECT shelve FROM prs.shelves')
    #rows=[{"shelve":"weila"},{"shelve":"weila"},{"shelve":"weila"}]
    dict={}
    for row in rows:
        hashv=abs(hash((row['shelve'])))% (10 ** 8)
        if(dict.has_key(hashv) is False):
           dict[hashv]=[]
        dict[hashv].append(row['shelve'])
    for entry in dict:
        if len(dict[entry])>1:
            print entry, dict[entry]

#Complete the users reviews with the shelves informations from the book table
def completeUsersReviews(c):
    while(True):
        rows = c.getSession().execute('SELECT id,reviews_retrieved FROM prs.users WHERE shelves_reviews_retrieved=false')
        for row in rows:
            if row['reviews_retrieved'] is False:
                logging.warning("Reviews retrieved false for user %s", row['id'])
                continue
            logging.info("Getting shelves for user %s", row['id'])
            user=models.Users.get(id=row['id'])
            all=True
            if(user.list_reviews is None):
                user.reviews_retrieved=False
                user.save()
                continue
            for i, reviews in enumerate(user.list_reviews):
                if(user.list_reviews[i].book.list_shelves is None or len(user.list_reviews[i].book.list_shelves)==0):
                    book=models.Books.get(id=reviews.book.id)
                    if(book.list_shelves is not None and len(book.list_shelves)>0):
                        for shelve in book.list_shelves:
                            user.list_reviews[i].book.list_shelves.append(shelve)
                    else:
                        logging.error("User %s with book %d id %s has no shelves", user.id, book.gid, book.id)
                        all=False
            if(all):
                 user.shelves_reviews_retrieved=True
            else:
                if(user.shelves_reviews_retrieved is None or user.shelves_reviews_retrieved is True):
                    user.shelves_reviews_retrieved=False
            user.save()
            logging.info("Finished retrieving shelves for user %s", user.id)
#Complete the users reviews with the shelves informations from the book table
def completeUsersReviewsOrEliminate(c):
    while(True):
        rows = c.getSession().execute('SELECT id, reviews_retrieved FROM prs.users where shelves_reviews_retrieved=false and reviews_retrieved=true limit 500 allow filtering')
        for row in rows:
            if row['reviews_retrieved'] is False:
                logging.warning("Reviews retrieved false for user %s", row['id'])
                continue
            logging.info("Getting shelves for user %s", row['id'])
            user=models.Users.get(id=row['id'])
            if(user.shelves_reviews_retrieved):
                logging.warning("User shelves already retrieved for user %s", row['id'])
                continue
            else:
                user.shelves_reviews_retrieved=True
                user.save()
            indexToRemove=[]
            if(user.list_reviews is None):
                user.reviews_retrieved=False
                user.shelves_reviews_retrieved=False
                user.save()
                continue
            for i, reviews in enumerate(user.list_reviews):
                if(user.list_reviews[i].book.list_shelves is None or len(user.list_reviews[i].book.list_shelves)==0):
                    book=models.Books.get(id=reviews.book.id)
                    if(book.list_shelves is not None and len(book.list_shelves)>0):
                        for shelve in book.list_shelves:
                            user.list_reviews[i].book.list_shelves.append(shelve)
                    else:
                        logging.error("User %s with book %d id %s has no shelves", user.id, book.gid, book.id)
                        indexToRemove.append(i)
            for i in sorted(indexToRemove, reverse=True):
                user.list_reviews.pop(i)
            user.shelves_reviews_retrieved=True
            user.save()
            logging.info("Finished retrieving shelves for user %s with %d deletions", user.id, len(indexToRemove))
        time.sleep(10)

#Check the coherence of the database on the "shelve reviews_retrieved" flags
def shelveRetrivedControl(c):
    rows = c.getSession().execute('SELECT id FROM prs.users')
    for row in rows:
        logging.info("Getting shelves for user %s", row['id'])
        user=models.Users.get(id=row['id'])
        for i, reviews in enumerate(user.list_reviews):
             if(user.list_reviews[i].book.list_shelves is None or len(user.list_reviews[i].book.list_shelves)==0):
                 user.shelves_reviews_retrieved=False
                 user.save()
                 break
             else:
                 break

#Remove reviews up to 500
def removeSomeReviews(c):
    rows = c.getSession().execute('SELECT id FROM prs.users where reviews_filtered=false')
    for row in rows:
        logging.info("Getting reviews for user %s", row['id'])
        user=models.Users.get(id=row['id'])
        list=user.list_reviews
        if(list is None):
            user.reviews_filtered=True
            user.save()
            continue
        list.sort(key=lambda r: r.rating, reverse=True)
        index=0
        for i, r in enumerate(list):
            if(r.rating<3):
                index=i
                break
        if(index>0):
            list[index:] = []
        list[500:] = []
        user.list_reviews=[]
        for r in list:
            ac=models.user(id=r.actor.id, gid=r.actor.gid, name=r.actor.name, friends_count=r.actor.friends_count, reviews_count= r.actor.reviews_count,
            age=r.actor.age, gender=r.actor.gender, small_user=r.actor.small_user, private=r.actor.private)
            bk=models.book(id=r.book.id, gid=r.book.gid,  title=ascii(r.book.title),isbn=r.book.isbn, isbn13=r.book.isbn13,
            publication_date=r.book.publication_date, average_rating=r.book.average_rating,
            ratings_count=r.book.ratings_count, small_book=r.book.small_book, author=r.book.author)
            user.list_reviews.append(models.review(id=r.id, actor=ac, book=bk, gid=r.gid,
                                            rating=r.rating, text=ascii(r.text)))
        user.reviews_count=len(user.list_reviews)
        user.reviews_filtered=True
        user.save()
        logging.info("Finish removing some reviews for user %s", row['id'])
#Set the current database as a test set
def makeUserTestSet(c):
    rows = c.getSession().execute('SELECT id FROM prs.users')
    for row in rows:
        c.getSession().execute('UPDATE prs.users SET test_set = true WHERE id=%s', [row['id']])

#Check the coherence of the flag shelve_retrieved on the books table
def bookShelvesRetrievedCheck(c):
    rows = c.getSession().execute('SELECT id FROM prs.books')
    for row in rows:
        book=models.Books.get(id=row['id'])
        if(book.list_shelves is None or len(book.list_shelves)==0):
            book.shelves_retrieved=False
            book.save()
        else:
            book.shelves_retrieved=True
            book.save()
#Check the coherence of the flag shelve_reviews_retrieved on the users table
def correctShelvesRetrieved(c):
    rows = c.getSession().execute('SELECT id FROM prs.users WHERE shelves_reviews_retrieved=true and reviews_retrieved=false ALLOW FILTERING')
    for row in rows:
        c.getSession().execute('UPDATE prs.users SET shelves_reviews_retrieved = false WHERE id=%s', [row['id']])


def setUSersReviewsRetrieved(c):
    rows = c.getSession().execute('SELECT id FROM prs.users')
    for row in rows:
        user=models.Users.get(id=row['id'])
        user.shelves_reviews_retrieved=False
        user.save()

#Check the coherence of the flag shelve_retrieved in the users table
def checkReviewsRetrieved(c):
    rows = c.getSession().execute('SELECT id FROM prs.users WHERE reviews_retrieved=true')
    for row in rows:
        user=models.Users.get(id=row['id'])
        if(user.list_reviews==None or len(user.list_reviews)<=0):
            logging.warning("User %s has no reviews", row['id'])
            user.reviews_retrieved=False
            user.shelves_reviews_retrieved=False
            user.save()
        else:
            logging.info("Shelve number %d", len(user.list_reviews))

#Check the coherence of the flag shelve_retrieved in the users table
def repairBookShelvesRetrieved(c):
    rows = c.getSession().execute('SELECT id FROM prs.books WHERE shelves_retrieved=true')
    for row in rows:
        book=models.Books.get(id=row['id'])
        if(len(book.list_shelves)==0):
            logging.info("Book %s has list shelves and retrieved is %r", book.id, book.shelves_retrieved )
        if(len(book.list_shelves)==0 and  book.shelves_retrieved):
            book.shelves_retrieved=False
            book.save()
#Check and repair the coherence of the dataset by check if users are retrieved correctly
def repairUserFriendsRetrieved(c):
    rows = c.getSession().execute('SELECT id FROM prs.users WHERE friends_retrieved=true')
    for row in rows:
        user=models.Users.get(id=row['id'])
        if(user.list_friends is None or len(user.list_friends)==0):
            logging.info("Book %s has list shelves and retrieved is %r", user.id, user.friends_retrieved )
        if((user.list_friends is None or len(user.list_friends)==0 ) and  user.friends_retrieved ):
            user.friends_retrieved=False
            user.save()

def setFriendsRetrievedWhenPrivate():
    rows = c.getSession().execute('SELECT id FROM prs.users WHERE private=true')
    for row in rows:
        user=models.Users.get(id=row['id'])
        user.friends_retrieved=True;
        user.save()

def setBookDetailsRetrieved(c):
    rows = c.getSession().execute('SELECT id, user_gid FROM prs.recommendation')
    for row in rows:
        c.getSession().execute('UPDATE prs.recommendation SET book_details_retrieved = false WHERE id=%s', [row['id']])

def setCommonShelveretrieved(c):
    rows = c.getSession().execute('SELECT id, common_shelves_retrieved,most_common_shelves FROM prs.recommendation')
    for row in rows:
        if(row['most_common_shelves'] is None):
            c.getSession().execute('UPDATE prs.recommendation SET common_shelves_retrieved = false WHERE id=%s', [row['id']])
        else:
            c.getSession().execute('UPDATE prs.recommendation SET common_shelves_retrieved = true WHERE id=%s', [row['id']])

#Get book description from the goodreads API
def getBooksDescription(c):
    rows = c.getSession().execute('SELECT user_gid FROM prs.recommendation WHERE book_details_retrieved=false')
    for row in rows:
        recom=models.Recommendation.get(user_gid= int(row['user_gid']))
        if(recom.books_details_recommended is not None):
            logging.warning("Book details aready retrieved for user %s", row['user_gid'])
            recom.book_details_retrieved=True
            recom.save()
            continue
        books = []
        logging.info("Getting book for user %d", recom.user_gid)
        for book_gid in recom.item_recommended:
            book=models.Books.get(gid=book_gid)
            books.append(books_to_book(book))
        logging.info("Retrieved %d books", len(books))
        recom.books_details_recommended = books
        recom.book_details_retrieved=True
        recom.save()


class Shelve(object):
    def __init__(self,shelve, votes, count):
        self.id=None
        self.gid=0
        self.shelve = shelve
        self.votes = votes
        self.count = count

#Set the top shelves to the recommended tables (for the Web UI)
def topShelvesToUsers(c):
    for i in range(0,10):
        for line in open("/Users/Simo/Google Drive/Master thesis/Document/MT-product-recommender-system-improved-with-social-netowrk-informations/Code/"+
                         "Prototype1/selected_results/2016-1-25-11-54-45-3000users_shelves_statistics/top_shelves/part-0000"+str(i)):
            elem=line.split("|")
            user_gid=elem[0].replace("(", "").split(" ")[0]
            print user_gid
            rows = c.getSession().execute('SELECT id FROM prs.recommendation WHERE user_gid=%s and common_shelves_retrieved=false allow filtering', [int(user_gid)])
            try:
                logging.info("Saving shelves for user %s", rows[0]['id'])
            except Exception as e:
                continue
            shelves=[]
            for s_v_c in elem[1:]:
                svc_array=s_v_c.replace("(", "").replace(")","").split(",")
                #print svc_array[0] #Shelve
                #print int(svc_array[1]) #Votes
                #print int(svc_array[2]) #Counts
                shelves.append(Shelve(svc_array[0], int(svc_array[1]), int(svc_array[2])))
            insert_statement = c.getSession().prepare("UPDATE  prs.recommendation  SET most_common_shelves = ?, common_shelves_retrieved= ? WHERE id= ?")
            c.getSession().execute(insert_statement, [shelves, True,  UUID(str(rows[0]['id']))])


def getUserName(c):
    rows = c.getSession().execute('SELECT id, user_gid FROM prs.recommendation')
    for row in rows:
        users=c.getSession().execute('SELECT id, name FROM prs.users WHERE gid=%s', [row['user_gid']])
        logging.info("Update username for user %s wth name %s", row['id'], users[0]['name'])
        c.getSession().execute('UPDATE  prs.recommendation  SET username = %s WHERE id= %s', [users[0]['name'], row['id']])

if __name__ == "__main__":
    initialize_logger(os.getcwd())
    coloredlogs.install(level='DEBUG')
    coloredlogs.ColoredFormatter()
    c=Cassandra()
    sync_table(models.Books)
    sync_table(models.Shelves)
    sync_table(models.Authors)
    sync_table(models.Users)
    sync_table(models.Reviews)
    sync_table(models.Recommendation)
    setCommonShelveretrieved(c)
