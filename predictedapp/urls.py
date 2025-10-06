from django.urls import path
from . import views

urlpatterns = [
    path('list', views.predicts, name='predicts'),
    path('download', views.dowload, name='dowload'),
    path('number', views.numberOfMatches, name='numberOfMatches'),
    path('delete/<str:matchId>', views.delete, name='delete'),
    path('delete_all', views.delete_all, name='delete_all')
]