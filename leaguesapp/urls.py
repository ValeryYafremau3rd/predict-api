from django.urls import path
from . import views

urlpatterns = [
    path('match', views.match, name='match'),
    path('<str:name>', views.teams, name='teams'),
    path('', views.leagues, name='leagues'),
]