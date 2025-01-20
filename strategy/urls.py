from django.urls import path
from . import views

urlpatterns = [
    path('predicts/<int:userId>', views.predicts, name='predicts'),
    path('download/<int:userId>', views.dowload_xml, name='dowload_xml'),
    path('queue/<int:userId>', views.add_to_queue, name='add_to_queue'),
    path('get_queue/<int:userId>', views.get_queue, name='get_queue'),
    path('delete_from_queue/<str:matchId>', views.delete_from_queue, name='delete_from_queue'),
    path('delete_from_results/<str:matchId>', views.delete_from_results, name='delete_from_results')
]