from django.urls import path
from . import views

urlpatterns = [
    path('delete/<str:matchId>', views.delete_from_queue, name='delete_from_queue'),
    path('delete_all', views.delete_all_from_queue, name='delete_all_from_queue'),
    path('add', views.add_to_queue, name='add_to_queue'),
    path('', views.get_queue, name='get_queue'),
]