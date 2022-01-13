from django.db import models
from django.contrib.auth.models import (
    BaseUserManager, AbstractBaseUser,
    UserManager)

class User(AbstractBaseUser):
    email = models.EmailField(max_length=127, unique=True, null=False, blank=False)
    is_staff = models.BooleanField(default=True)
    is_active = models.BooleanField(default=True)
    is_superuser=models.BooleanField(default=True)
    storage_alerts = models.BooleanField(default=True)
    etl_alerts = models.BooleanField(default=True)
    feedback_alerts = models.BooleanField(default=True)
    username = models.TextField(max_length=127, unique=True, null=False, blank=False)

    first_name = models.TextField(max_length=127, unique=True, null=False, blank=False)
    last_name = models.TextField(max_length=127, unique=True, null=False, blank=False)
    objects = UserManager()

    USERNAME_FIELD = "email"
    # REQUIRED_FIELDS must contain all required fields on your User model,
    # but should not contain the USERNAME_FIELD or password as these fields will always be prompted for.
    REQUIRED_FIELDS = ['username']


    def __str__(self):
        return self.email

    def get_full_name(self):
        return self.email

    def get_short_name(self):
        return self.email

    # this methods are require to login super user from admin panel
    def has_perm(self, perm, obj=None):
        return self.is_staff

    # this methods are require to login super user from admin panel
    def has_module_perms(self, app_label):
        return self.is_staff
    # notice the absence of a "Password field", that is built in.

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username'] # Email & Password are required by default.
    def create_user(self, username, email=None, password=None, **extra_fields):
        extra_fields.setdefault('is_staff', False)
        extra_fields.setdefault('is_superuser', False)
        return self._create_user(username, email, password, **extra_fields)

    def create_superuser(self, email, is_staff,is_superuser, username,password,storagealerts,etlalerts,feedbackalerts,first_name,last_name):
        user = self.model(
            email=email,
            is_staff=is_staff,
            is_superuser=is_superuser,
            username=username,
            storage_alerts=storagealerts,
            etl_alerts=etlalerts,
            feedback_alerts=feedbackalerts,
            first_name=first_name,
            last_name=last_name
        )
        user.set_password(password)
        user.save(using=self._db)
        return user