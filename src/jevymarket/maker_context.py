"""Read-only existing-log context for a supplied v6 diagnostic; never starts a run.

python -m jevymarket.maker_context --db maker.db --diagnostic diag.json.gz --out context.json.gz
The diagnostic identifies the session explicitly, even if the DB has newer runs.
No orders, arbitrary event bodies, credentials or database paths are exported.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sqlite3
from pathlib import Path

MAX_INPUT = 8_000_000
MAX_ROW = 256_000
MAX_OUTPUT = 12_000_000
KINDS = ('book_event', 'book_reject', 'source_error', 'market', 'observation')
FOCUS = {'bbo_delta_mismatch', 'delta_without_snapshot_or_out_of_order', 'out_of_order_snapshot'}
PUBLIC_KEYS = ('event_type', 'market', 'asset_id', 'timestamp', 'price', 'size', 'side', 'hash',
               'transaction_hash', 'fee_rate_bps', 'old_tick_size', 'new_tick_size', 'best_bid', 'best_ask')
ROW_KEYS = ('asset_id', 'price', 'size', 'side', 'best_bid', 'best_ask', 'hash')


def scalar(value):
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value[:512]
    if type(value) in (int, float) and math.isfinite(value):
        return value
    return None


def project(data, keys):
    if not isinstance(data, dict):
        return {}
    result, changed = {}, []
    for key in keys:
        if key not in data:
            continue
        value = data[key]
        result[key] = scalar(value)
        if isinstance(value, str) and len(value) > 512:
            changed.append(key + ':string_truncated')
        elif value is not None and result[key] is None:
            changed.append(key + ':invalid_scalar')
    if changed:
        result['context_sanitization'] = changed
    return result


def public_event(data):
    data = data if isinstance(data, dict) else {}
    result = project(data, PUBLIC_KEYS)
    for key in ('bids', 'asks', 'price_changes'):
        if key in data:
            rows = data[key]
            result[key] = [project(row, ROW_KEYS) for row in rows[:2048]] if isinstance(rows, list) else None
            if isinstance(rows, list) and len(rows) > 2048:
                result.setdefault('context_truncation', []).append(key)
    if 'diagnostic_sanitization' in data:
        result['source_was_sanitized'] = True
    return result


def safe_event(kind, data):
    if not isinstance(data, dict):
        raise ValueError('数据库事件不是JSON对象')
    if kind == 'book_event':
        return public_event(data)
    if kind == 'book_reject':
        result = project(data, ('reason', 'session_id', 'io_revision', 'slug', 'received_ts', 'received_mono'))
        result['event'] = public_event(data.get('event'))
        # The original diagnostic already includes focus before/candidate states.
        return result
    if kind == 'source_error':
        return project(data, ('stage', 'code', 'reason', 'io_revision'))
    if kind == 'market':
        return project(data, ('slug', 'condition', 'start', 'window', 'up_token', 'down_token'))
    return project(data, ('ts', 'slug', 'reason', 'session_id', 'io_revision', 'observe_only'))


def read_diagnostic(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rb') as handle:
        raw = handle.read(MAX_INPUT + 1)
    if len(raw) > MAX_INPUT:
        raise ValueError('诊断文件超过8MB解压上限；请使用小型diagnose导出')
    data = json.loads(raw)
    if data.get('format') != 'v6-book-diagnostic-r3':
        raise ValueError('需要v6-book-diagnostic-r3诊断文件')
    return data, hashlib.sha256(raw).hexdigest()


def extract_context(db: Path, diagnostic: Path, *, before: int = 2000, after: int = 512) -> dict:
    if not 0 <= before <= 4000 or not 0 <= after <= 2000:
        raise ValueError('上下文范围超限')
    db, diagnostic = Path(db).resolve(), Path(diagnostic).resolve()
    if not db.is_file():
        raise ValueError('数据库不存在；不会创建空数据库')
    supplied, input_hash = read_diagnostic(diagnostic)
    summary = supplied['summary']
    start_id = int(summary['start_event_id'])
    session = summary['runtime']['session_id']
    examples = [e for e in supplied['reject_examples'] if e['data']['reason'] in FOCUS][:6]
    if not examples:
        raise ValueError('诊断中没有本工具所需的BBO/乱序样例')
    conn = sqlite3.connect(db.as_uri() + '?mode=ro', uri=True, timeout=5)
    try:
        conn.execute('PRAGMA query_only=ON')
        conn.execute('BEGIN')
        row = conn.execute("SELECT kind,data FROM maker_events WHERE id=?", (start_id,)).fetchone()
        if not row or row[0] != 'runtime' or json.loads(row[1]).get('session_id') != session:
            raise ValueError('数据库与诊断会话不匹配；停止导出')
        next_run = conn.execute("SELECT min(id) FROM maker_events WHERE kind='runtime' AND id>?", (start_id,)).fetchone()[0]
        end_id = next_run - 1 if next_run else conn.execute('SELECT max(id) FROM maker_events').fetchone()[0]
        focuses, bounds, wanted = [], [], set()
        for example in examples:
            event_id = int(example['event_id'])
            row = conn.execute('SELECT kind,data FROM maker_events WHERE id=?', (event_id,)).fetchone()
            if not row or row[0] != 'book_reject' or not start_id <= event_id <= end_id:
                raise ValueError('找不到对应拒绝事件；停止导出')
            original = json.loads(row[1])
            expected = example['data']
            if any(original.get(k) != expected.get(k) for k in ('session_id', 'reason', 'event')):
                raise ValueError('拒绝帧与诊断不匹配；停止导出')
            focuses.append({'event_id': event_id, 'reason': original['reason']})
            lo, hi = max(start_id, event_id - before), min(end_id, event_id + after)
            bounds.append({'focus_id': event_id, 'first_id': lo, 'last_id': hi})
            wanted.update(range(lo, hi + 1))
        output, omitted, used = [], [], 0
        # Integer primary-key bounds avoid reading the entire book stream.
        for lo, hi in merge_ranges(bounds):
            query = ("SELECT id,ts,kind,CASE WHEN length(CAST(data AS BLOB))<=? THEN data ELSE NULL END "
                     "FROM maker_events WHERE id BETWEEN ? AND ? AND kind IN "
                     "('book_event','book_reject','source_error','market','observation') ORDER BY id")
            for event_id, ts, kind, raw in conn.execute(query, (MAX_ROW, lo, hi)):
                if raw is None:
                    omitted.append({'event_id': event_id, 'reason': 'row_byte_limit'})
                    continue
                value = {'event_id': event_id, 'stored_ts': ts, 'kind': kind,
                         'data': safe_event(kind, json.loads(raw))}
                size = len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8'))
                if used + size > MAX_OUTPUT - 200_000:
                    omitted.append({'event_id': event_id, 'reason': 'total_byte_limit'})
                    continue
                output.append(value)
                used += size
        return {'format': 'v6-book-context-r1', 'diagnostic_filename': diagnostic.name,
                'diagnostic_uncompressed_sha256': input_hash, 'session_id': session,
                'start_event_id': start_id, 'end_event_id': end_id, 'focuses': focuses, 'bounds': bounds,
                'events': output, 'omitted': omitted, 'selected_kinds': list(KINDS),
                'exported_events': len(output), 'requested_id_positions': len(wanted),
                'limits': {'row_bytes': MAX_ROW, 'output_bytes': MAX_OUTPUT, 'array_rows': 2048},
                'limitations': [
                    '只导出指定旧会话中已存事件；没有重新采集、联网、结算、订单操作或源库写入',
                    '事件按数据库id排序；stored_ts不是精确WS到达/交易所撮合时间',
                    '仅白名单字段；字符串最多512字符，数组最多2048行，超大行或总量省略会标记',
                    '失败前/候选盘口使用原诊断；本文件不重复复制它们',
                    '旧库没有的WS帧边界、连接编号、已丢弃后续消息无法补回',
                    '有界上下文未必始于完整快照，不能据此假设缺失深度或保证可完整重建',
                ]}
    finally:
        conn.close()


def merge_ranges(bounds):
    merged = []
    for lo, hi in sorted((b['first_id'], b['last_id']) for b in bounds):
        if merged and lo <= merged[-1][1] + 1:
            merged[-1][1] = max(hi, merged[-1][1])
        else:
            merged.append([lo, hi])
    return merged


def write_context(report, out):
    payload = json.dumps(report, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')
    if len(payload) > MAX_OUTPUT:
        raise ValueError('导出超出12MB未压缩上限；缩小上下文范围')
    out = Path(out)
    opener = gzip.open if out.suffix == '.gz' else open
    with opener(out, 'xb') as handle:
        handle.write(payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--diagnostic', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--before', type=int, default=2000)
    parser.add_argument('--after', type=int, default=512)
    args = parser.parse_args()
    try:
        if args.out.exists():
            raise ValueError('输出文件已存在，不覆盖')
        report = extract_context(args.db, args.diagnostic, before=args.before, after=args.after)
        write_context(report, args.out)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        parser.exit(1, f'只读导出失败：{exc}\n')
    print(f"只读导出完成：{args.out.name}；事件{report['exported_events']}；省略{len(report['omitted'])}；没有启动采集")


if __name__ == '__main__':
    main()
