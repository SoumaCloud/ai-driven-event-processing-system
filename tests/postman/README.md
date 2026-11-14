# Postman API suite

`ai-event-processor.postman_collection.json` calls every service's health, metrics and main endpoints (ingestion, forecasting, diagnostic, event-archiver) and checks the responses. URLs point at the local stack's host ports (`make up`); change the collection variables for another environment.

Run it with [newman](https://github.com/postmanlabs/newman):

```bash
npx newman run tests/postman/ai-event-processor.postman_collection.json
# HTML report (gitignored):
npx -p newman -p newman-reporter-htmlextra newman run tests/postman/ai-event-processor.postman_collection.json \
  -r cli,htmlextra --reporter-htmlextra-export tests/postman/report.html
```

The "Override threshold" request lowers ingestion-service's `cpu_utilisation` threshold so the forecast run raises an alert. Reset it afterwards (`PUT /thresholds/ingestion-service`) if you keep using the stack.
