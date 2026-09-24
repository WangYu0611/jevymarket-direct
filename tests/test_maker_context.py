import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from jevymarket.maker_context import extract_context, merge_ranges, public_event, write_context


def fixture_db(tmp_path: Path):
    db = tmp_path / '诊断 data.db'
    conn = sqlite3.connect(db)
    conn.execute('CREATE TABLE maker_events (id INTEGER PRIMARY KEY,ts REAL,kind TEXT,data TEXT)')
    event = {'event_type': 'price_change', 'market': 'c', 'timestamp': '100000',
             'price_changes': [{'asset_id': 't', 'price': '.5', 'size': '7', 'side': 'BUY'}]}
    reject = {'session_id': 'old', 'reason': 'bbo_delta_mismatch', 'event': event, 'secret': 'DO_NOT_EXPORT'}
    values = [(1, 'runtime', {'session_id': 'old'}), (2, 'book_event', event),
              (3, 'book_reject', reject), (4, 'source_error', {'stage': 'orderbook', 'headers': 'PRIVATE'}),
              (5, 'book_event', dict(event, timestamp='100001')),
              (6, 'runtime', {'session_id': 'new'}), (7, 'book_event', dict(event, market='NEW_SESSION'))]
    conn.executemany('INSERT INTO maker_events VALUES (?,?,?,?)',
                     [(i, float(i), kind, json.dumps(data)) for i, kind, data in values])
    conn.commit()
    conn.close()
    diagnostic = tmp_path / 'diag.json.gz'
    report = {'format': 'v6-book-diagnostic-r3',
              'summary': {'start_event_id': 1, 'runtime': {'session_id': 'old'}},
              'reject_examples': [{'event_id': 3, 'data': reject}]}
    with gzip.open(diagnostic, 'wt', encoding='utf-8') as handle:
        json.dump(report, handle)
    return db, diagnostic, report


def test_read_only_session_scope_and_whitelist(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    checksum = hashlib.sha256(db.read_bytes()).hexdigest()
    report = extract_context(db, diagnostic)
    assert report['session_id'] == 'old'
    assert report['end_event_id'] == 5
    assert [e['event_id'] for e in report['events']] == [2, 3, 4, 5]
    assert 'DO_NOT_EXPORT' not in json.dumps(report)
    assert 'PRIVATE' not in json.dumps(report)
    assert 'NEW_SESSION' not in json.dumps(report)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == checksum


def test_missing_database_not_created(tmp_path):
    absent = tmp_path / 'absent.db'
    with pytest.raises(ValueError, match='不存在'):
        extract_context(absent, tmp_path / 'absent.gz')
    assert not absent.exists()


def test_mismatched_session_rejected(tmp_path):
    db, diagnostic, report = fixture_db(tmp_path)
    report['summary']['runtime']['session_id'] = 'wrong'
    with gzip.open(diagnostic, 'wt') as handle:
        json.dump(report, handle)
    with pytest.raises(ValueError, match='会话不匹配'):
        extract_context(db, diagnostic)


def test_mismatched_reject_rejected(tmp_path):
    db, diagnostic, report = fixture_db(tmp_path)
    report['reject_examples'][0]['data']['event']['timestamp'] = '999'
    with gzip.open(diagnostic, 'wt') as handle:
        json.dump(report, handle)
    with pytest.raises(ValueError, match='拒绝帧'):
        extract_context(db, diagnostic)


def test_no_overwrite(tmp_path):
    out = tmp_path / 'out.json.gz'
    write_context({'test': 1}, out)
    saved = out.read_bytes()
    with pytest.raises(FileExistsError):
        write_context({'test': 2}, out)
    assert out.read_bytes() == saved


def test_zero_context_only_reject(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    report = extract_context(db, diagnostic, before=0, after=0)
    assert [e['event_id'] for e in report['events']] == [3]


def test_bounds_and_duplicate_ranges():
    assert merge_ranges([{'first_id': 2, 'last_id': 5}, {'first_id': 4, 'last_id': 8},
                         {'first_id': 10, 'last_id': 12}]) == [[2, 8], [10, 12]]


def test_large_row_explicitly_omitted(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute('UPDATE maker_events SET data=? WHERE id=2', (json.dumps({'pad': 'x' * 300000}),))
    conn.commit()
    conn.close()
    report = extract_context(db, diagnostic)
    assert report['omitted'] == [{'event_id': 2, 'reason': 'row_byte_limit'}]


def test_public_projection_and_array_bound():
    event = public_event({'headers': {'key': 'PRIVATE'}, 'price_changes': [
        {'price': '.4', 'size': '5', 'credential': 'PRIVATE'}] * 2050})
    assert len(event['price_changes']) == 2048
    assert event['context_truncation'] == ['price_changes']
    assert 'PRIVATE' not in json.dumps(event)


def test_wal_open_writer(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute("INSERT INTO maker_events VALUES (8,8,'book_event','{}')")
    conn.commit()
    try:
        assert extract_context(db, diagnostic)['end_event_id'] == 5
    finally:
        conn.close()


def test_range_limit(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    with pytest.raises(ValueError, match='范围超限'):
        extract_context(db, diagnostic, before=4001)


def test_gzip_roundtrip(tmp_path):
    db, diagnostic, _ = fixture_db(tmp_path)
    report = extract_context(db, diagnostic)
    out = tmp_path / 'output.gz'
    write_context(report, out)
    with gzip.open(out, 'rt', encoding='utf-8') as handle:
        assert json.load(handle) == report


def test_scalar_changes_marked():
    event = public_event({'hash': 'a' * 600, 'price': float('inf')})
    assert len(event['hash']) == 512
    assert event['price'] is None
    assert event['context_sanitization'] == ['price:invalid_scalar', 'hash:string_truncated']


def test_byte_limit_no_output(tmp_path, monkeypatch):
    from jevymarket import maker_context
    monkeypatch.setattr(maker_context, 'MAX_OUTPUT', 5)
    out = tmp_path / 'over.gz'
    with pytest.raises(ValueError, match='上限'):
        write_context({'large': 'payload'}, out)
    assert not out.exists()
