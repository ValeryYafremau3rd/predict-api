from django.urls import path
from . import views

urlpatterns = [
    path('list', views.predicts, name='predicts'),
    path('download', views.dowload, name='dowload'),
    path('delete/<str:matchId>', views.delete, name='delete')
]