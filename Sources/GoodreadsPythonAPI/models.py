__author__ = 'Simo'
from cassandra.cqlengine.columns import UUID, Text, Double, DateTime, List, Integer, Boolean, UserDefinedType
from cassandra.cqlengine.models import Model
import uuid
from cassandra.cqlengine.usertype import UserType

#-----------------------------------
# This class provide the model of the Goodreads
# dataset on the Cassandra database
#-----------------------------------

class author(UserType):
    id=UUID(default=uuid.uuid4())
    name = Text(required=True, index=True)
    gid=Integer(required=True,index=True)
    age= Integer()
    gender= Text()
    location=Text()
    interests=Text()
    born_at=DateTime()
    diet_at=DateTime()
    friends_count= Integer()
    reviews_count= Integer()
    fans_count=Integer()
    author_followers_count=Integer()
    works_count=Integer()
    average_rating=Double()
    ratings_count=Integer()
    text_reviews_count=Integer()
    small_author=Text()

class shelve(UserType):
    #id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid=Integer(index=True)
    shelve=Text(required=True,index=True)
    votes=Integer(index=True)
    count=Integer(index=True)
    best=Boolean(default=False)

class book(UserType):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid=Integer(required=True, index=True)
    title=Text(required=True, index=True)
    isbn=Text(index=True)
    isbn13=Text()
    publication_date=DateTime(index=True)
    description=Text()
    average_rating=Double(required=True, index=True)
    ratings_count=Integer(required=True)
    reviews_count=Integer()
    small_book=Boolean()# no shelves, review_count, description, smilar_book
    author=UserDefinedType(author, default=author())
    list_shelves=List(UserDefinedType(shelve))
    error_retrieving_shelves=Boolean(default=False)


class user(UserType):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid = Integer(required=True,index=True)
    name = Text(required=True,index=True)
    friends_count= Integer()
    reviews_count= Integer()
    age= Integer(index=True)
    gender= Text(index=True)
    location=Text(index=True)
    small_user=Boolean(default=False, index=True)
    private=Boolean(default=False,index=True)

class tb_tag(UserType):
    word=Text()
    type=Text()

class tb_sentence(UserType):
    sentence_text=Text()
    sentence_polarity=Double()
    sentence_subjectivity=Double()

#Type that define a textblob type resulting from the textblob tool
class textblob(UserType):
    instantiated=Boolean()
    tags=List(UserDefinedType(tb_tag))
    noun_phrases=List(Text)
    sentences=List(UserDefinedType(tb_sentence))


class review(UserType):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    actor=UserDefinedType(user)
    book=UserDefinedType(book)
    gid=Integer(required=True,index=True)
    rating=Integer(required=True, index=True)
    text=Text()
    comments_count=Integer()
    textblob=UserDefinedType(textblob)




class Shelves(Model):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid=Integer()
    shelve=Text(required=True,index=True)
    count=Integer()
    list_book=List(UserDefinedType(book))


class Books(Model):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid=Integer(primary_key=True,required=True, index=True)
    title=Text(required=True, index=True)
    isbn=Text()
    isbn13=Text()
    publication_date=DateTime(index=True)
    description=Text()
    average_rating=Double(required=True, index=True)
    ratings_count=Integer(required=True)
    reviews_count=Integer()
    small_book=Boolean(default=True, index=True)# no shelves, review_count, description, smilar_book
    author=UserDefinedType(author,default=author())
    #Specific to model
    reviews_retrieved=Boolean(default=False, required=False, index=True)
    shelves_retrieved=Boolean(default=False, required=False, index=True)
    #Lists
    list_shelves=List(UserDefinedType(shelve), required=False)
    list_reviews=List(UserDefinedType(review), required=False)


class Authors(Model):
    id=UUID(primary_key=True, default=uuid.uuid4(), partition_key=True)
    name = Text(required=True, index=True)
    gid=Integer(required=True,index=True)
    age= Integer(index=True)
    gender= Text(index=True)
    location=Text(index=True)
    interests=Text()
    born_at=DateTime(index=True)
    diet_at=DateTime(index=True)
    friends_count= Integer()
    reviews_count= Integer()
    fans_count=Integer()
    author_followers_count=Integer()
    works_count=Integer()
    average_rating=Double()
    ratings_count=Integer()
    text_reviews_count=Integer()
    #Lists
    list_books=List(UserDefinedType(book))

class Users(Model):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    gid = Integer(required=True,index=True)
    name = Text(required=True,index=True)
    friends_count= Integer()
    reviews_count= Integer(index=True)
    age= Integer(index=True)
    gender= Text(index=True)
    location=Text(index=True)
    small_user=Boolean(default=False, index=True)
    private=Boolean(default=False,index=True)
    #specific to model
    friends_retrieved=Boolean(default=False,index=True)
    reviews_retrieved=Boolean(default=False,index=True)
    shelves_reviews_retrieved=Boolean(default=False,index=True)
    reviews_filtered=Boolean(default=False,index=True)
    test_set=Boolean(default=False,index=True)

    #Lists
    list_reviews=List(UserDefinedType(review))
    list_friends=List(UserDefinedType(user))
    short_list_reviews=List(UserDefinedType(review))
    most_common_shelves=List(UserDefinedType(shelve), required=False)

class Reviews(Model):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    actor=UserDefinedType(user, default=user())
    book=UserDefinedType(book, default=book(author=author()))
    gid=Integer(required=True,index=True)
    rating=Integer(required=True, index=True)
    text=Text()
    comments_count=Integer()
    textblob=UserDefinedType(textblob, default=textblob(instantiated=False))

#Class where the recommendation will be saved
class Recommendation(Model):
    id=UUID(primary_key=True, default=uuid.uuid4, partition_key=True)
    user_gid=Integer(required=True, index=True)
    username=Text()
    user_image=Text()
    item_recommended= List(Integer)
    books_details_recommended=List(UserDefinedType(book))
    precision=Double(default=0.0, index=True)
    recall=Double(default=0.0,index=True)
    book_details_retrieved=Boolean(default=False, index=True)
    most_common_shelves=List(UserDefinedType(shelve), required=False)
    common_shelves_retrieved=Boolean(default=False,index=True)