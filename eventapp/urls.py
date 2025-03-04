from django.urls import path
from . import views

urlpatterns = [
    path('operations', views.operations, name='operations'),
    path('create', views.create, name='create'),
    path('list', views.event_list, name='event_list'),
    path('delete/<str:id>', views.delete, name='delete'),
    path('edit/<str:id>', views.edit, name='edit'),
    path('odd/<str:id>', views.odd, name='odd'),
]