import shutil
import smtplib
from pathlib import Path

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.utils import timezone

from api.models import Storage_Review, Profile


class Command(BaseCommand):
    help = 'send email when threshold is reached.'

    # Function Handler
    def handle(self, *args, **options):
        list = Storage_Review.objects.all()
        for l in list:
            stat = shutil.disk_usage(l.directory)
            free = stat.free
            size = sum(p.stat().st_size for p in Path(l.directory).rglob('*'))
            bsize=size
            bthreshold=l.threshold*1024*1024
            for funit in ("B", "K", "M", "G", "T"):
                if free < 1024:
                    break
                free /= 1024
            for sunit in ("B", "K", "M", "G", "T"):
                if size < 1024:
                    break
                size /= 1024
            free_str = str(round(free, 2)) + funit
            used_str = str(round(size, 2)) + sunit
            try:
                # if used size in bytes exceeds threshold size in bytes for a path, an email will be sent notifying the available space
                if bsize > bthreshold:
                    SUBJECT = "ClimateSERV2.0 memory threshold reached!!"
                    TEXT = "This email informs you that the memory usage in the path "+l.directory+" has reached "+used_str+" and the free space available is "+free_str+"."

                    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
                    #get users registered for storgae alerts
                    user_arr=[]
                    storage_user_arr=[]
                    admin=[]
                    for profile in Profile.objects.all():
                        if profile.storage_alerts:
                            user_arr.append(profile.user)
                    for user in User.objects.all():
                        for us in user_arr:
                            if str(us)==str(user.username):
                                storage_user_arr.append(user.email)
                    #retrieving admin credentials that were given in the Django admin interface with username as "email_admin"
                    for user in User.objects.all():
                        if str(user.username) == "email_admin":
                            for p in Profile.objects.all():
                                if str(p.user)=="email_admin":
                                    admin.append(user.email)
                                    admin.append(p.gmail_password)
                                    break
                            break
                    # Send storage alert emails for all subscribed users
                    for storage_user in storage_user_arr:
                        mail = smtplib.SMTP('smtp.gmail.com', 587)
                        mail.ehlo()
                        mail.starttls()
                        mail.login(admin[0], admin[1])
                        storage_Review = Storage_Review.objects.get(directory=l.directory)
                        storage_Review.last_notified_time=timezone.now()
                        storage_Review.free_space = free_str
                        storage_Review.file_size = used_str
                        storage_Review.save()
                        mail.sendmail(admin[0], storage_user, message)
                        mail.close()
                        self.stdout.write(self.style.SUCCESS(
                            'notify_usage.py: {} {}'.format(storage_user,"email sent")))
            except Exception as e:
                print(e)
        return