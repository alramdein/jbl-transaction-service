from django.utils import timezone
from clickhouse_backend import models

class Transaction(models.ClickhouseModel):
    user_id = models.Int32Field()
    product_id = models.Int32Field()
    amount = models.Float32Field()
    timestamp = models.DateTime64Field(default=timezone.now)

    class Meta:
        ordering = ["-timestamp"]
        engine = models.MergeTree(
            primary_key="timestamp",
            order_by=("timestamp", "user_id"),
            partition_by=models.toYYYYMMDD("timestamp"),
            index_granularity=1024,
            index_granularity_bytes=1 << 20,
            enable_mixed_granularity_parts=1,
        )
