"""Engine-independent canonical row and SQL Server schema contract."""
from __future__ import annotations
import hashlib
from decimal import Decimal, InvalidOperation
import json
import source_catalog as catalog
COLUMNS = ('id', 'contract_code', 'amount', 'status', 'description')
COLUMN_TYPES = ('bigint', 'text', 'numeric(14,2)', 'text', 'text')

def canonical(value):
    """A conservative content contract; no lossy string/float coercion.

    SQL Server DECIMAL and PostgreSQL NUMERIC become exact 2-decimal strings.
    TEXT/NVARCHAR are compared exactly as Unicode (no trim/case folding).
    Only the explicitly supported column types are permitted.
    """
    if isinstance(value, dict):
        obj = value
    else:
        if len(value) != len(COLUMNS):
            raise ValueError('Unsupported row projection')
        obj = dict(zip(COLUMNS, value))
    if set(obj) != set(COLUMNS):
        raise ValueError('Different row fields, schema migration needed')
    id_ = obj['id']
    if isinstance(id_, bool) or not isinstance(id_, int) or not 1 <= id_ <= 2**63 - 1:
        raise ValueError('Invalid positive BIGINT identifier')
    try:
        amount = Decimal(obj['amount'])
        if not amount.is_finite() or amount.as_tuple().exponent < -2:
            raise ValueError('Invalid decimal scale or non-finite amount')
        amount = amount.quantize(Decimal('0.01'))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('Amount cannot be represented exactly as NUMERIC(14,2)') from exc
    if amount == 0:
        amount = abs(amount)
    if abs(amount) >= Decimal('1000000000000'):
        raise ValueError('NUMERIC(14,2) exceeds 12 integral digits')
    for key in ('contract_code', 'status', 'description'):
        val = obj[key]
        if key != 'description' and val is None:
            raise ValueError('Unexpected NULL: ' + key)
        if val is not None and not isinstance(val, str):
            raise ValueError('Non-text value in ' + key)
        if val is not None:
            # Refuse malformed surrogate sequences, never silently replace.
            val.encode('utf-8', errors='strict')
    return {'id': id_, 'contract_code': obj['contract_code'],
            'amount': format(amount, '.2f'), 'status': obj['status'],
            'description': obj['description']}


def fingerprint(row):
    # Canonical bytes are independent of PostgreSQL jsonb::text and T-SQL's
    # HASHBYTES encoding and of Python's driver-specific Decimal formatting.
    import json
    document = canonical(row)
    payload = json.dumps(document, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def validate_native(manifest):
    """Fail closed on metadata drift/unsupported type or source PK semantics."""
    if manifest.get('engine') != 'sqlserver' or manifest.get('primary_key') != ['id']:
        raise RuntimeError('SQL Server contract must have a single BIGINT primary key id')
    cols = manifest['columns']
    if [c['name'] for c in cols] != list(COLUMNS):
        raise RuntimeError('SQL Server schema drift: columns/order differ from approved data contract')
    expected_nullable = (False, False, False, False, True)
    for c, expected_type, nullable in zip(cols, COLUMN_TYPES, expected_nullable):
        if c['logical_type'] != expected_type or c['nullable'] != nullable:
            raise RuntimeError('SQL Server schema drift: incompatible type/nullability in ' + c['name'])
        if c['computed'] or c['generated_always_type'] or (c['identity'] and c['name'] != 'id'):
            raise RuntimeError('Computed/generated/unsupported identity column: ' + c['name'])
    return True


def portable_manifest(native):
    """Bridge SQL Server catalog signature into existing V5 schema registry."""
    return {'table': native['table'], 'source_engine': 'sqlserver',
            'native_hash': catalog.sha(native),
            'primary_key': ['id'], 'indexes': [], 'constraints': [],
            'columns': [
                {'name': name, 'type': typ, 'nullable': is_nullable,
                 'default': None, 'identity': '', 'generated': ''}
                for name, typ, is_nullable in zip(
                    COLUMNS, COLUMN_TYPES, (False, False, False, False, True))
            ]}


