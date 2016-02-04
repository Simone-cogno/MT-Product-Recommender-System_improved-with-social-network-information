from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^user/(?P<user_id>[0-9]+)/$', views.user, name='user'),
    url(r'^book/(?P<book_id>[0-9]+)/$', views.book, name='book'),
]