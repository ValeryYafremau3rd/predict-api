from django.urls import path
from . import views

urlpatterns = [
    path('list', views.group_list, name='group_list'),
    path('create', views.create, name='create'),
    path('delete/<str:id>', views.delete, name='delete'),
    path('<str:id>', views.search, name='search'),
]