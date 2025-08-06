# Zillow Short Sale Bot

This project contains a FastAPI server that processes datasets from Apify and pushes qualifying properties to a Google Sheet.

## Running the server

Use the `run.sh` script to start the webhook server. The script loads environment variables from `.env` if the file exists and then launches `uvicorn` on port `8000`:

```bash
./run.sh
```

## Apify integration

Configure your Apify actor to send a POST request to the `/apify-hook` endpoint of this server when your dataset is ready. The webhook should include the dataset ID, either in the JSON body or as a `datasetId` query parameter.

Example JSON body:

```json
{ "datasetId": "YOUR_DATASET_ID" }
```

Apify will POST this payload to:

```
https://<your-server-host>/apify-hook
```

On receiving the dataset ID, the server will fetch the dataset and process any new rows.

