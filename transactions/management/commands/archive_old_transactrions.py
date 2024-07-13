from django.core.management.base import BaseCommand
from transactions.views import archive_old_transactions

class Command(BaseCommand):
    help = 'Archive old transactions to S3 Minio'

    def handle(self, *args, **kwargs):
        archive_old_transactions()
        self.stdout.write(self.style.SUCCESS('Successfully archived old transactions'))
