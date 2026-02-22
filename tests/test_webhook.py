import os
from fastapi.testclient import TestClient
from unittest.mock import patch
import importlib

# Ensure required env vars for module import
os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("SMSM_KEY", "test")
os.environ.setdefault("SHEET_URL", "http://example.com")

import webhook_server


def reset_module_state():
    webhook_server.EXPORTED_ZPIDS.clear()
    importlib.reload(webhook_server)


def get_client():
    return TestClient(webhook_server.app)


import pytest


@pytest.mark.asyncio
async def test_hook_with_json():
    reset_module_state()
    with patch('webhook_server.fetch_rows') as mock_fetch_rows, \
         patch('webhook_server.process_rows') as mock_process_rows:
        mock_fetch_rows.return_value = [{'zpid': '111'}]
        client = get_client()

        resp = client.post('/apify-hook', json={'datasetId': 'ds1'})
        assert resp.status_code == 200
        assert resp.json() == {'status': 'ok', 'imported': 1}
        mock_fetch_rows.assert_called_once_with('ds1')
        mock_process_rows.assert_called_once_with([{'zpid': '111'}])

        export_resp = client.get('/export-zpids')
        assert export_resp.status_code == 200
        assert export_resp.json() == ['111']


@pytest.mark.asyncio
async def test_hook_with_query():
    reset_module_state()
    with patch('webhook_server.fetch_rows') as mock_fetch_rows, \
         patch('webhook_server.process_rows') as mock_process_rows:
        mock_fetch_rows.return_value = [{'zpid': '222'}]
        client = get_client()

        resp = client.post('/apify-hook?datasetId=ds2', json={})
        assert resp.status_code == 200
        assert resp.json() == {'status': 'ok', 'imported': 1}
        mock_fetch_rows.assert_called_once_with('ds2')
        mock_process_rows.assert_called_once_with([{'zpid': '222'}])

        export_resp = client.get('/export-zpids')
        assert export_resp.status_code == 200
        assert export_resp.json() == ['222']


