from django.urls import path
from . import views

urlpatterns = [
    path('teams/<str:id>', views.team, name='teams'),
]