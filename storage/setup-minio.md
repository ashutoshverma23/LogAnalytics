# MinIO Setup (S3 Compatible Storage)

This project uses MinIO as an S3-compatible object store.

Buckets:
- raw-logs           → raw Kafka events
- processed-logs     → Spark-processed data

Default credentials (dev only):
- Access key: admin
- Secret key: admin123

Ports:
- API: 9000
- Console: 9001