from django.urls import path
from .views import get_transactions, get_transaction_details, create_transaction, update_transaction, delete_transaction

urlpatterns = [
    path('transactions/', get_transactions, name='get_transactions'),
    path('transactions/create/', create_transaction, name='create_transaction'),
    path('transactions/<str:sku>/', get_transaction_details, name='get_transaction_details'),
    path('transactions/update/<str:sku>/', update_transaction, name='update_transaction'),
    path('transactions/delete/<str:sku>/', delete_transaction, name='delete_transaction'),
]
