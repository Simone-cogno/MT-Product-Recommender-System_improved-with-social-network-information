__author__ = 'Simo'
from outils import *
import xml.etree.ElementTree as ET
import models
import datetime
import uuid
from types import ListType, StringType
from bs4 import BeautifulSoup
from textblob import TextBlob
import re

#Parse a book from the Goodreads API xml result
def parseBook(xmlstring):
    root = ET.fromstring(xmlstring)
    dic= etree_to_dict(root)
    x=dict2obj(dic)
    b=x.GoodreadsResponse.book
    return _createBookBig(b)

#Create a book with all the information
def _createBookBig(book):
    data=None
    year=None
    month=1
    day=1
    if(book.publication_year):
       year= int(book.publication_year)
    if(book.publication_month):
        month=int(book.publication_month)
    if(book.publication_day):
        day=int(book.publication_day)
    if(year is None):
        data=None
    else:
        try:
            data=datetime.datetime(year,month,day)
        except Exception as e:
            data=None

    bookJson = {'id':uuid.uuid4(),
        'gid': int(book.id),
         'title': ascii(book.title),
         'isbn': ascii(book.isbn),
         'isbn13': ascii(book.isbn13),
         'publication_date': data,
         'description': ascii(book.description),
         'average_rating': float(book.average_rating),
         'ratings_count': int(book.ratings_count),
         'reviews_count': int(book.text_reviews_count),
         'list_shelves':_parseShelves(book.popular_shelves),
         'small_book': False,
         'author':authors_to_author(_parseAuthorsBig(book.authors))}
    bookTable=models.Books(**bookJson)
    try:
        similar_books=_parseSimilarBooks(book.similar_books.book)
    except KeyError as e:
        logging.error(e.message)
        return (bookTable, None)
    except:
        logging.error( "Not possible to retrieve similar books of %d", int(book.id))
        return (bookTable, None)
    else:
        return (bookTable,similar_books)

#Create a book with only the basic information
def _createBookSmall(book):#no description, shelves, publication date
    bookJson = {'id':uuid.uuid4(),
         'gid': int(book.id),
         'title': ascii(book.title_without_series),
         'isbn': book.isbn,
         'isbn13': book.isbn13,
         'average_rating': float(book.average_rating),
         'ratings_count': int(book.ratings_count),
         'author':authors_to_author(_parseAuthorsSmall(book.authors))}
    bookTable=models.Books(**bookJson)
    return bookTable
#Parse the shelves from teh Goodreads API
def _parseShelves(popular_shelves):
    if(isinstance(popular_shelves, StringType)):
        logging.error("%s", popular_shelves)
        return []
    listShelves=[]
    shelf=popular_shelves.shelf
    if isinstance(shelf, ListType):
        for s in shelf:
            shelve={
                 'id':uuid.uuid4(),
                 'gid': 1,
                 'shelve': s['@name'],
                 'count': int(s['@count'])
            }
            listShelves.append(models.shelve(**shelve))
        return listShelves
    else:
        shelve={
                 'id':uuid.uuid4(),
                 'gid': 1,
                 'shelve': shelf['@name'],
                 'count': int(shelf['@count'])
        }
        listShelves.append(models.shelve(**shelve))
        return listShelves

#Parse a user from the Goodreads API xml result
def parseUser(xmlstring):
    root = ET.fromstring(xmlstring)
    dic= etree_to_dict(root)
    x=dict2obj(dic)
    obj=x.GoodreadsResponse.user
    return _createUserBig(obj)

#Create a user model with all the details
def _createUserBig(obj):
    u=models.Users()
    u.gid=int(obj.id)
    u.name=ascii(obj.name)
    u.location = ascii(obj.location)
    u.age = int(obj.age)
    try:#TODO: create a list of AuthorType
        list=[]
        for a in obj.favorite_authors:
            list.append(_parseAuthorSmall(a.author))
        u.favorite_authors=list
    except AttributeError:
        u.favorite_authors = None
    u.gender = ascii(obj.gender)
    u.interests = ascii(obj.interests)
    u.friends_count = int(obj.friends_count['#text'])
    u.reviews_count = int(obj.reviews_count['#text'])
    return u
#Create a user model with the basic information
def _createUserSmall(obj):
    u=models.Users()
    u.id=uuid.uuid4()
    u.gid=int(obj.id)
    u.name=ascii(obj.name)
    u.friends_count = int(obj.friends_count)
    u.reviews_count = int(obj.reviews_count)
    u.small_user=True
    return u
#Parse an author from Goodreads API
def _parseAuthorsBig(authors):
    if isinstance(authors.author,ListType):
        for a in authors.author:
            return _parseAuthorBig(a)
    else:
        return _parseAuthorBig(authors.author)

#Parse an author with the basic information
def _parseAuthorsSmall(authors):
    if isinstance(authors,ListType):
        for a in authors.author:
            return _parseAuthorSmall(a)
    else:
        return _parseAuthorSmall(authors.author)

def _parseAuthorSmall(obj):
    return models.Authors(id=uuid.uuid4(), gid=obj.id, name=ascii(obj.name))


def _parseAuthorBig(obj):
    return models.Authors(gid=obj.id,
                  name=ascii(obj.name),
                  average_rating=float(obj.average_rating),
                  ratings_count=int(obj.ratings_count),
                  text_reviews_count=int(obj.text_reviews_count))


def _parseSimilarBooks(similar_books):
    listSimilarBook=[]
    for book in similar_books:
        book=_createBookSmall(book)
        listSimilarBook.append(book)
    return listSimilarBook


#Parse the users reviews for the HTML page of Goodreads.com
def parseUserReviews(g, html, user):
    soup = BeautifulSoup(html, 'html.parser')

    reviews=[]
    books=[]
    authors=[]

    table = soup.find('table', attrs={'id':'books'})
    table_body = table.find('tbody')

    rows = table_body.find_all('tr')
    for row in rows:
        review_id=int(re.search(r'\d+', row.get('id')).group())
        div_title= row.find('td', attrs={'class':'title'}).find('div', attrs={'class':'value'})
        title=div_title.getText().replace("  ","")
        book_id=int(re.search(r'\d+', div_title.find('a').get('href')).group())
        author_name_forname= row.find('td', attrs={'class':'field author'}).find('a').text.replace(' ','').split(',')
        if(len(author_name_forname)>1):
            author_name=author_name_forname[1]+" "+author_name_forname[0]
        else:
            author_name=author_name_forname[0] #TODO give two colum for name and forname
        author_id= int(re.search(r'\d+', row.find('td', attrs={'class':'field author'}).find('a').get('href')).group())
        isbn= row.find('td', attrs={'class':'field isbn'}).find('div',attrs={'class':'value'}).text.replace("  ","")
        isbn13= row.find('td', attrs={'class':'field isbn13'}).find('div',attrs={'class':'value'}).text.replace("  ","")
        avg_rating= float(row.find('td', attrs={'class':'field avg_rating'}).find('div',attrs={'class':'value'}).text.replace("  ",""))
        num_ratings= int(row.find('td', attrs={'class':'field num_ratings'}).find('div',attrs={'class':'value'}).text.replace(",",""))
        date_pub_string= row.find('td', attrs={'class':'field date_added'}).find('div',attrs={'class':'value'}).text.replace("  ","")
        date_pub= stringToDatetime(date_pub_string)
        field_rating= row.find('td', attrs={'class':'field rating'})
        a_staticStars=field_rating.find('a', attrs={'class':'staticStars'})
        rating=None
        if(a_staticStars is None):
            stars=field_rating.find('div', attrs={'class':'stars'})
            if(stars is None):
                p10=field_rating.findAll('span', attrs={'class':'p10'})
                rating=len( p10)
            else:
                rating=int(stars.get('data-rating'))
        else:
            #rating=int(a_staticStars['class'][2][-1])
            rating=len(a_staticStars.find('span', attrs={'class':'p10'}))
        if(rating<3):
            continue
        spans= row.find('td', attrs={'class':'review'}).findAll('span')
        review=''
        if(len(spans)>1):
            review=ascii(spans[1].getText())
        elif(len(spans)>0):
            review=ascii(spans[0].getText())
        else:
            review=None

        #Author
        authorTable=models.Authors(id=uuid.uuid4(), gid=author_id, name=ascii(author_name))
        authors.append(authorTable)
        #Book
        bookTable=models.Books(id=uuid.uuid4(), gid=book_id,  title=ascii(title),isbn=ascii(isbn), isbn13=ascii(isbn13),
                       publication_date=date_pub,average_rating=avg_rating,ratings_count=num_ratings)
        books.append(bookTable)
        #Review
        textblob=None
        if(review is not None):
            blob=TextBlob(ascii(review))
            # tags=[]
            # for tag in blob.tags:
            #     tags.append(models.tb_tag(word=re.sub(r'[^\x00-\x7F]+',' ', tag[0]), type=re.sub(r'[^\x00-\x7F]+',' ', tag[0])))
            #noun_phrases=[]
            #for noun_phrase in blob.noun_phrases:
            #    noun_phrases.append(re.sub(r'[^\x00-\x7F]+',' ', noun_phrase))
            sentences=[]
            for sentence in blob.sentences:
                if(sentence.string is None):
                    continue
                sentences.append(models.tb_sentence(sentence_text=ascii(sentence.string), sentence_polarity=sentence.polarity, sentence_subjectivity=sentence.subjectivity))
            textblob=models.textblob(instantiated=True, sentences=sentences)
        else:
            textblob=models.textblob(instantiated=False)

        reviewTable=models.Reviews(id=uuid.uuid4(), actor=user, gid=review_id,  rating=rating, text=review, textblob=textblob)
        reviews.append(reviewTable)
    return (reviews,books, authors)

#Parse user friends
def parseUserFriends(xml):
    root = ET.fromstring(xml)
    dic= etree_to_dict(root)
    x=dict2obj(dic)
    try:
        friends=x.GoodreadsResponse.friends.user
    except Exception, e:
        print xml
        raise e
    else:
        return (parseListFriends(friends), int(x.GoodreadsResponse.friends['@total']))


#Parse the list of freinds retrieved from Goodreads API
def parseListFriends(friends):
    if isinstance(friends,ListType):
        listFriends=[]
        for i in friends:
            user=_createUserSmall(i)
            listFriends.append(user)
        return listFriends
    else:
        return [_createUserSmall(friends)]
