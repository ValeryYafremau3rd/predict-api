from django.db import models

class teams(models.Model):
  form = models.CharField(max_length=255)



#class League(models.Model):
#  id = models.SmallIntegerField()
#  name = models.CharField(max_length=255)
#  season = models.SmallIntegerField(max_length=255)
#
#class TeamName(models.Model):
#  id = models.SmallIntegerField()
#  name = models.CharField(max_length=255)