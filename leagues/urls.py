from django.urls import path
from . import views

urlpatterns = [
    path('leagues', views.leagues, name='leagues'),
    path('leagues/<str:name>', views.teams, name='teams'),
]