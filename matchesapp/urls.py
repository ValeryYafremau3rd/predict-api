from django.urls import path
from . import views

urlpatterns = [
    path('matches/<str:id>', views.match, name='matches'),
]