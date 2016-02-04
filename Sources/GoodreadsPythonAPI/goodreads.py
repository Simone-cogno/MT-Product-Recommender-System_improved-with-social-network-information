#coding: utf-8
__author__ = 'Simo'
import urllib
from parser import parseBook, parseUser, parseUserReviews, parseUserFriends
from cassandracustom import Cassandra
import datetime
from models import Books, Users
import oauth2 as oauth
import urlparse
import requests
from bs4 import BeautifulSoup
import time
import logging
from outils import *


#This class is responsable for parse the results of the Goodreads API and create a Cassandra model to save in the
#Cassandra database
class Goodreads:
    #Initialize the secret oauth token
    def __init__(self, client=None, new=False):
        self.key=''
        self.secret=''
        self.oauth_token=''
        self.oauth_token_secret=''
        self.client=client
        self.connect(new)
        self.lastTime={'book':None,'friends_page':None}
    #Connect to Goodreads API with Oauth 2
    #Snippet retrieved from https://gist.github.com/steve-kertes/5862716 and https://gist.github.com/gpiancastelli/537923
    def connect(self, new=False):
        url = 'http://www.goodreads.com'
        request_token_url = '%s/oauth/request_token' % url
        authorize_url = '%s/oauth/authorize' % url
        access_token_url = '%s/oauth/access_token' % url

        consumer = oauth.Consumer(self.key,
                                  self.secret)
        if not new:
            token = oauth.Token(self.oauth_token,
                                self.oauth_token_secret)
            self.client=oauth.Client(consumer, token)
            return

        self.client = oauth.Client(consumer)

        response, content = self.client.request(request_token_url, 'GET')
        if response['status'] != '200':
            raise Exception('Invalid response: %s, content: ' % response['status'] + content)

        request_token = dict(urlparse.parse_qsl(content))

        authorize_link = '%s?oauth_token=%s' % (authorize_url,
                                                request_token['oauth_token'])
        print "Use a browser to visit this link and accept your application:"
        print authorize_link
        accepted = 'n'
        while accepted.lower() == 'n':
            # you need to access the authorize_link via a browser,
            # and proceed to manually authorize the consumer
            accepted = raw_input('Have you authorized me? (y/n) ')

        token = oauth.Token(request_token['oauth_token'],
                            request_token['oauth_token_secret'])

        self.client = oauth.Client(consumer, token)
        response, content = self.client.request(access_token_url, 'POST')
        if response['status'] != '200':
            raise Exception('Invalid response for connect(): %s' % response['status'])

        access_token = dict(urlparse.parse_qsl(content))

        # this is the token you should save for future uses
        print 'Save this for later: '
        print 'oauth token key:    ' + access_token['oauth_token']
        print 'oauth token secret: ' + access_token['oauth_token_secret']

        token = oauth.Token(access_token['oauth_token'],
                            access_token['oauth_token_secret'])
        self.client=oauth.Client(consumer, token)


    def getBook(self, book_id):
        #self.waitIfNecessary('book',ls='')
        body = urllib.urlencode({'id': book_id})
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        try:
            response, content = self.client.request('https://www.goodreads.com/book/show.xml', 'GET', body, headers)
        except Exception as e:
            logging.error(e.message+" for book %d", book_id)
            return None, None
        if response['status'] != '200':
            logging.error('Invalid request: %s' % response['status'])
            return None, None
        else:
            return parseBook(content)


    def getUser(self, user_id):
        body = urllib.urlencode({'id': user_id})
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        response, content = self.client.request('https://www.goodreads.com/user/show.xml', 'GET', body, headers)
        if response['status'] != '200':
            raise Exception('Invalid request: %s' % response['status'])
        else:
            return parseUser(content)

    def getPageReviews(self, user, page,total):
        logging.info('get review for user '+str(user.gid)+' page '+str(page)+' of '+str(total))
        cookies=dict(session_id2='',
        p='',
        u='-6IY_')
        url = 'https://www.goodreads.com/review/list/'+str(user.gid)+'-stephanie?order=d&page='+str(page)+'&per_page=100&sort=rating&shelf=read&utf8=✓&view=reviews'
        try:
            r = requests.get(url, cookies=cookies)
            if r.status_code != 200:
                logging.warning('Invalid request: %s' % r.status_code)
                return ([],[], [])
            else:
                return parseUserReviews(self, r.content, user)
        except Exception as e:
            logging.error('Exception on page reviews')
            return ([],[], [])

    #Get the reviews from the web page of Goodreads.com
    def getUserReviews(self, user):
        logging.info( 'Get user reviews for %d id: %s:', user.gid, user.id)

        cookies=dict(session_id2='',
        p='',
        u='')
        url = 'https://www.goodreads.com/review/list/'+str(user.gid)+'-stephanie?order=d&per_page=100&shelf=read&sort=rating&utf8=✓&view=reviews'
        r = requests.get(url, cookies=cookies)
        if r.status_code != 200:
            logging.warning('Invalid request: %s' % r.status_code)
            return (None, False)
        else:
            soup = BeautifulSoup(r.content, 'html.parser')
            if(soup.find('div', attrs={'id': 'privateProfile'})):
                logging.warning('User %d is private', user.gid)
                return (None, False)
            divPagination=soup.find('div', attrs={'id':'reviewPagination'})
            if(divPagination is None):
                reviews, books, authors=parseUserReviews(self, r.content, users_to_user(user))
                return ({'reviews': reviews, 'books': books, 'authors': authors}, False)
            pages_a=divPagination.findAll('a')
            pages=[]
            for a in pages_a:
                class_a=a.get('class')
                if(class_a is None or(class_a[0]!='next_page' and class_a[0]!= 'previous_page')):
                    pages.append({'num':int(a.text), 'link': a.get('href')})
            all_reviews=[]
            all_books=[]
            all_authors=[]

            reviews, books, authors=parseUserReviews(self, r.content, users_to_user(user))
            all_reviews.append(reviews)
            all_books.append(books)
            all_authors.append(authors)
            if(len(pages)>1):
                total_pages_count=pages[-1]['num']+1
                if(total_pages_count>6):
                    total_pages_count=6
                for p in range(2,total_pages_count):
                    reviews, books, authors= self.getPageReviews(users_to_user(user), p, total_pages_count)
                    all_reviews.append(reviews)
                    all_books.append(books)
                    all_authors.append(authors)
            all_reviews = [val for sublist in all_reviews for val in sublist]
            all_books=[val for sublist in all_books for val in sublist]
            all_authors=[val for sublist in all_authors for val in sublist]
            logging.info( '\nFinish getting %d reviews %d books %d authors',len(all_reviews), len(all_books), len(all_authors))
            return ({'reviews': all_reviews, 'books': all_books, 'authors': all_authors}, False)


    #Get a friends list page
    def getFriendsPage(self, user, page=1, ls=''):
        self.waitIfNecessary('friends_page',ls=ls)
        # the book is: "Generation A" by Douglas Coupland
        body = urllib.urlencode({'id': user.gid,'page': page})
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        try:
            response, content = self.client.request('https://www.goodreads.com/friend/user.xml', 'GET', body, headers)
        except Exception as e:
            return (None, 0)
        else:
            if response['status'] != '200':
                logging.error(ls+'Invalid request error: %s getting friends of %d id: %s',response['status'], user.gid, user.id)
                return (None,0)
            else:
                friends, total= parseUserFriends(content)
                return (friends, total)

    #Get all friends of an user
    def getFriends(self, user, ls):
        all_friends=[]
        friends, total=self.getFriendsPage(user, page=1, ls=ls)
        if(friends is None):
            return (None, 0)
        logging.info(ls+'(tatal %d)',total)
        for friend in friends:
            all_friends.append(friend)
        #total_pages=total/30.0
        #if(total_pages>1):
        #    if(total_pages%1==0):
        #        total_pages=int(total_pages)-1
        #    logging.info(ls+'pages %d',int(total_pages)+1)
        #    for i in range(2,3):
        #        print i,
        #        try:
        #            friends, total=self.getFriendsPage(user, page=i, ls=ls)
        #        except Exception, e:
        #            print e.message
        #        break
        #        else:
        #            if(friends is None):
        #                return (None, 0)
        #            for friend in friends:
        #                all_friends.append(friend)
        return all_friends, total

    #Assure that we don't make more than 1 requesto pro seconds
    def waitIfNecessary(self,type, ls):
        if(self.lastTime[type] is None):
            self.lastTime[type]=datetime.datetime.now()
        else:
            now=datetime.datetime.now()
            deltatime=now-self.lastTime[type]
            if(deltatime.total_seconds()<1):
                logging.debug(ls+"sleep %.2f seconds", (1-deltatime.total_seconds()))
                time.sleep(1-deltatime.total_seconds())
            self.lastTime[type]=datetime.datetime.now()

