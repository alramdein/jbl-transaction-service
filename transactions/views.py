from django.http import JsonResponse
from django.core.paginator import Paginator
from clickhouse_driver import Client as ClickHouseClient
from minio import Minio
from datetime import datetime, timedelta
from .models import Transaction
import json
import os

clickhouse_client = ClickHouseClient(host='localhost')
minio_client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

def get_transactions(request):
    if request.method != 'GET':
        return JsonResponse({"error": "Invalid HTTP method."}, status=405)
    
    page = request.GET.get('page', 1)
    try:
        page = int(page)
    except ValueError:
        return JsonResponse({"error": "Invalid page number."}, status=400)

    try:
        # Fetch all transactions from ClickHouse
        transactions = clickhouse_client.execute('SELECT sku, qty, amount FROM transactions')

        # Paginate transactions
        paginator = Paginator(transactions, 10)
        try:
            transactions_page = paginator.page(page)
        except EmptyPage:
            return JsonResponse({"message": "No more pages."}, status=404)

        # Convert transactions_page to a list of dictionaries for JSON serialization
        transactions_list = []
        for transaction in transactions_page:
            transactions_list.append({
                'sku': transaction[0],   # SKU
                'qty': transaction[1],   # QTY
                'amount': transaction[2]  # Amount
            })

        # Return the paginated transactions as JSON response
        return JsonResponse(transactions_list, safe=False)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

def get_transaction_details(request, sku):
    if request.method != 'GET':
        return JsonResponse({"error": "Invalid HTTP method."}, status=405)
    
    transaction = clickhouse_client.execute('SELECT sku, qty, amount FROM transactions WHERE sku = %(sku)s', {'sku': sku})
    if not transaction:
        return JsonResponse({"message": "Transaction not found."}, status=404)
    transaction = transaction[0]
    return JsonResponse({
            'sku': transaction[0],
            'qty': transaction[1],
            'amount': transaction[2]
        })

def create_transaction(request):
    if request.method != 'POST':
        return JsonResponse({"error": "Invalid HTTP method."}, status=405)
    
    data = json.loads(request.body)
    sku = data['sku']
    qty = data['qty']
    amount = data['amount']
    clickhouse_client.execute('INSERT INTO transactions (sku, qty, amount) VALUES', [(sku, qty, amount)])
    return JsonResponse({"message": "Transaction created."}, status=201)

def update_transaction(request, sku):
    if request.method != 'PUT':
        return JsonResponse({"error": "Invalid HTTP method."}, status=405)
    
    data = json.loads(request.body)
    qty = data.get('qty')
    amount = data.get('amount')
    
    transaction = clickhouse_client.execute('SELECT sku, qty, amount FROM transactions WHERE sku = %(sku)s', {'sku': sku})
    if not transaction:
        return JsonResponse({"message": "Transaction not found."}, status=404)

    try:
        clickhouse_client.execute('ALTER TABLE transactions UPDATE qty = %(qty)s, amount = %(amount)s WHERE sku = %(sku)s', {'qty': qty, 'amount': amount, 'sku': sku})
        return JsonResponse({"message": "Transaction updated."}, status=200)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

from django.http import JsonResponse
import json

def delete_transaction(request, sku):
    if request.method != 'DELETE':
        return JsonResponse({"error": "Invalid HTTP method."}, status=405)

    # Fetch the existing transaction to check if it exists
    transaction = clickhouse_client.execute('SELECT sku, qty, amount FROM transactions WHERE sku = %(sku)s', {'sku': sku})
    if not transaction:
        return JsonResponse({"message": "Transaction not found."}, status=404)

    # If transaction exists, delete it
    try:
        clickhouse_client.execute('ALTER TABLE transactions DELETE WHERE sku = %(sku)s', {'sku': sku})
        return JsonResponse({"message": "Transaction deleted."}, status=200)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

def archive_old_transactions():
    threshold_date = datetime.now() - timedelta(days=7)
    old_transactions = clickhouse_client.execute('SELECT * FROM transactions WHERE timestamp < %s', (threshold_date,))
    
    if old_transactions:
        file_name = f"transactions_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        with open(file_name, 'w') as f:
            json.dump(old_transactions, f)

        minio_client.fput_object(
            'transactions',
            file_name,
            file_name
        )

        os.remove(file_name)
        
        clickhouse_client.execute('ALTER TABLE transactions DELETE WHERE timestamp < %s', (threshold_date,))
