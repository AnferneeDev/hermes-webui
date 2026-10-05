"""Recovered Stop copies keep the same Gateway prefix as real public GET."""
import json
import subprocess
import sys

import pytest

from tests.test_recovered_export_share_and_gateway_http import _request, _seed, TOKENS
from tests.test_recovered_surrogate_http import _new_server


@pytest.mark.parametrize('operation', ['branch-all', 'fork-prefix', 'duplicate'])
@pytest.mark.parametrize('gateway', ['before-restart', 'after-carrier'])
def test_recovered_stop_copy_keeps_gateway_display_context_and_private_ids(tmp_path, operation, gateway):
    root, env, sid, stream, journal, raw = _seed(tmp_path, 'stop', TOKENS[3], gateway)
    with _new_server(env, root, tmp_path/'copy-server.log') as base:
        status, body, _ = _request(base, '/api/session?session_id='+sid+'&msg_limit=all')
        assert status == 200
        visible = json.loads(body)['session']['messages']
        keep = len(visible)-2 if operation == 'fork-prefix' else len(visible)
        expected = [row.get('content') for row in visible[:keep]]
        assert 'LATER_GATEWAY_REQUEST' in expected and 'LATER_GATEWAY_ANSWER' in expected
        endpoint = '/api/session/duplicate' if operation == 'duplicate' else '/api/session/branch'
        payload = {'session_id': sid}
        if operation != 'duplicate':
            payload['keep_count'] = keep
        status, body, _ = _request(base, endpoint, payload)
        assert status == 200, body
        response = json.loads(body)
        copied_id = response['session']['session_id'] if operation == 'duplicate' else response['session_id']
        for _ in range(2):
            status, body, _ = _request(base, '/api/session?session_id='+copied_id+'&msg_limit=all')
            assert status == 200
            rows = json.loads(body)['session']['messages']
            assert [row.get('content') for row in rows] == expected
            assert not any(key.startswith('_state_db') or key == '_row_id' for row in rows for key in row)
    inspect = r'''
import json,sys
from api import models,session_ops
session=models.Session.load(sys.argv[1])
assert session is not None
print(json.dumps({'display':session.messages,'context':session.context_messages,
                 'regeneration':session_ops.regeneration_state(session,use_sidecar=True)}))
'''
    result = subprocess.run([sys.executable, '-c', inspect, copied_id], cwd=root, env=env,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    saved = json.loads(result.stdout)
    assert [row.get('content') for row in saved['display']] == expected
    for rows in [saved['context'], *saved['regeneration']]:
        text = [row.get('content') for row in rows]
        assert text.count('LATER_GATEWAY_REQUEST') == text.count('LATER_GATEWAY_ANSWER') == 1
        assert ('SECOND_GATEWAY_ANSWER' in text) == (operation != 'fork-prefix')
        assert 'CANCELLED_RUN_REPLAY' not in text and 'CANCELLED_TOOL_REPLAY' not in text
        gateway_rows = [row for row in rows if 'GATEWAY_' in str(row.get('content'))]
        assert all(row.get('_state_db_row_id', 0) > 0 for row in gateway_rows)
    assert journal.read_bytes() == raw
