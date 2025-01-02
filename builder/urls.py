from django.urls import path
from . import views

urlpatterns = [
    path('builder/operations', views.operations, name='operations'),
    path('builder/create', views.create, name='create'),
    path('builder/list/<int:userId>', views.oddList, name='oddList'),
    path('builder/group/list/<int:userId>', views.groupList, name='groupList'),
    path('builder/delete/<str:id>', views.delete, name='delete'),
    path('builder/edit/<str:id>', views.edit, name='edit'),
    path('builder/odd/<str:id>', views.odd, name='odd'),
    path('builder/group/create', views.groupCreate, name='groupCreate'),
    path('builder/group/delete/<str:id>', views.groupDelete, name='groupDelete'),
    path('builder/group/<str:id>', views.getGroup, name='getGroup'),
]