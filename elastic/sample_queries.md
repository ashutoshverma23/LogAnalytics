# Sample Elasticsearch Queries

## Count logs by level
GET logs-*/_search
{
  "size": 0,
  "aggs": {
    "levels": {
      "terms": {
        "field": "level"
      }
    }
  }
}

## High latency requests
GET logs-*/_search
{
  "query": {
    "range": {
      "latency_ms": {
        "gt": 2000
      }
    }
  }
}