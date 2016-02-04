__author__ = 'Simo'
from collections import defaultdict
import datetime
import logging
import os.path
import models
import re
import uuid

#Convert a dictionary to an object source: http://stackoverflow.com/questions/1305532/convert-python-dict-to-object
class dict2obj(dict):
    def __init__(self, dict_):
        super(dict2obj, self).__init__(dict_)
        for key in self:
            item = self[key]
            if isinstance(item, list):
                for idx, it in enumerate(item):
                    if isinstance(it, dict):
                        item[idx] = dict2obj(it)
            elif isinstance(item, dict):
                self[key] = dict2obj(item)

    def __getattr__(self, key):
        return self[key]

#Convert an XML to dict source: http://stackoverflow.com/questions/7684333/converting-xml-to-dictionary-using-elementtree
def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.iteritems():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.iteritems()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.iteritems())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

#Convert a Mon string to number
def monthToNumber(str):
    month_num={
        'Jan' : 1,
        'Feb' : 2,
        'Mar' : 3,
        'Apr' : 4,
        'May' : 5,
        'Jun' : 6,
        'Jul' : 7,
        'Aug' : 8,
        'Sep' : 9,
        'Oct' : 10,
        'Nov' : 11,
        'Dec' : 12}
    return month_num[str]

#Perse a string to datetime
def stringToDatetime(str):
    str=str[2:]
    month_str=str[:3]
    day=str[4:6]
    year=str[-4:]
    return datetime.datetime(month=monthToNumber(month_str), day=int(day), year=int(year))

#used for print a space in the logs
def space(level):
    str=''
    for p in range(0,level-1):
        str+="\t"
    return str

if __name__ == "__main__":
    print stringToDatetime('Mar 16, 2011')


#Initialize the output log file and the colors of the debug levels
def initialize_logger(output_dir):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)



    # create error file handler and set level to error
    handler = logging.FileHandler(os.path.join(output_dir, "error.log"),"w", encoding=None, delay="true")
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, "info.log"),"w")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    handler = logging.FileHandler(os.path.join(output_dir, "all.log"),"w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
#Conver a string to ascii
def ascii(string):
    if(string is not None):
        return str(re.sub(r'[^\x00-\x7F]+',' ', string))
    else:
        return None
#Convert a table Users to type user
def users_to_user(user):
    return models.user(id=user.id, gid=user.gid, friends_count=user.friends_count, reviews_count= user.reviews_count,
                age=user.age, gender=user.gender,
                location=user.location,small_user=user.small_user, private=user.private)

#Convert a table Books to type book
def books_to_book(book):
    return models.book(id=book.id, gid=book.gid,  title=book.title,isbn=book.isbn, isbn13=book.isbn13,
                publication_date=book.publication_date,average_rating=book.average_rating,
                ratings_count=book.ratings_count, small_book=book.small_book, author=book.author, list_shelves=book.list_shelves)

#Convert a table Authors to type author
def authors_to_author(author):
    return models.author(id=author.id, gid=author.gid, name=author.name)

#Convert a table Reviews to type review
def reviews_to_review(review):
    return models.review(id=review.id, gid=review.gid, actor=review.actor, book=review.book,
                         rating=review.rating, text=review.text, comments_count=review.comments_count)

#Convert a table Shelves to type shelve
def shelve_to_shelves(shelve):
    return models.Shelves(id=uuid.uuid4(),gid=shelve.gid, shelve=shelve.shelve, count=shelve.count)