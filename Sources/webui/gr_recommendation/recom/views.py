from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader


from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from cassandra.cluster import Cluster
import models
from django.http import HttpResponse
from django.db import connection
import logging
logger = logging.getLogger(__name__)
from gender_detector import GenderDetector
import re

def connect():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('prs')
  
def index(request):
    connect()
    cursor = connection.cursor()
    rows = cursor.execute("SELECT id, user_gid, precision, recall, username, gender FROM prs.recommendation where common_shelves_retrieved=true") 
    users=sorted(rows, key=lambda k: k['precision'], reverse=True) 
    detector = GenderDetector('us')
    for i, user in enumerate(users):
        name=user['username'].replace('[', '').split(" ")
        gender=""
        try:
            gender=detector.guess(name[0])
        except Exception as e:
            gender="unknown"

        new_user=dict(models.Recommendation( user_gid=user['user_gid'], gender=gender,precision=user['precision'], recall=user['recall'], username=user['username']))
        users[i]=new_user
    return render(request, 'recom/index.html', {'users': users})

def user(request, user_id): 
    connect()
    user=dict(models.Recommendation.get(user_gid=user_id))
    name=user['username'].replace('[', '').split(" ")
    detector = GenderDetector('us')
    gender=""
    try:
        gender=detector.guess(name[0])
    except Exception as e:
        gender="unknown"
    user['gender']=gender
    for j, book in enumerate(user['books_details_recommended']):
        for i, shelve in enumerate(book['list_shelves']):
            for best_shelve in user['most_common_shelves']:
                if(best_shelve.shelve==shelve.shelve):
                    last_shelve=user['books_details_recommended'][j]['list_shelves'][i]
                    new_shelve=models.shelve(count=last_shelve.count,votes=last_shelve.votes, gid=last_shelve.gid,
                                             best=True,shelve=last_shelve.shelve)
                    user['books_details_recommended'][j]['list_shelves'][i]=new_shelve
    return render(request, 'recom/user.html', {'user': user}) 

def book(request, book_id):
    connect()
    users = [dict(user) for user in Books.objects(gid=book_id)]
    return render(request, 'recom/book.html', {'users': users}) 


