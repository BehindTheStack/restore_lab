"""Offline canonicalization/DDL policies. No Kafka/SQL Server required."""
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))
from canonical_contract import canonical, fingerprint, validate_native, portable_manifest

remote = (1, 'CT-á😀', Decimal('00100.20'), 'ACTIVE', None)
local = {'id': 1, 'contract_code': 'CT-á😀', 'amount': '100.20',
         'status': 'ACTIVE', 'description': None}
assert fingerprint(remote) == fingerprint(local)
assert canonical(remote) == canonical(local)
assert canonical((2, 'a', Decimal('-0.00'), 'active', 'X'))['amount'] == '0.00'
assert fingerprint((1, 'ABC', Decimal('100.20'), 'active', None)) != fingerprint(remote)
assert fingerprint((1, 'CT-á😀', Decimal('100.20'), 'active', None)) != fingerprint(remote)
assert fingerprint((1, 'CT-á😀', Decimal('100.21'), 'ACTIVE', None)) != fingerprint(remote)
assert fingerprint((1, 'CT-á😀', Decimal('100.20'), 'ACTIVE', '')) != fingerprint(remote)

for value in [Decimal('NaN'), Decimal('Infinity'), Decimal('1.234'), Decimal('1000000000000.00')]:
    try:
        canonical((1, 'a', value, 'a', 'a'))
        raise AssertionError(f'Bad decimal accepted: {value}')
    except ValueError:
        pass
try:
    canonical((1, 'a', Decimal('1.00'), 'a', '\ud800'))
    raise AssertionError('Invalid Unicode surrogate accepted')
except UnicodeEncodeError:
    pass

cols=[]
for idx,(name,logical,nullable) in enumerate([
    ('id','bigint',False), ('contract_code','text',False),
    ('amount','numeric(14,2)',False),('status','text',False),
    ('description','text',True)],1):
    cols.append({'name':name,'logical_type':logical,'nullable':nullable,
        'computed':False, 'identity':False, 'generated_always_type':0})
native={'engine':'sqlserver','table':'dbo.contracts','columns':cols,
        'primary_key':['id'],'indexes':[],'constraints':[]}
assert validate_native(native)
assert validate_native({**native, 'columns': [{**cols[0], 'identity': True}] + cols[1:]})
assert portable_manifest(native)['columns'][2]['type']=='numeric(14,2)'
for broken in [
    {**native,'primary_key':[]},
    {**native,'columns':cols+[{'name':'added','logical_type':'text','nullable':True}]},
    {**native,'columns':cols[:1]+[{**cols[1],'identity':True}]+cols[2:]},
    {**native,'columns':cols[:2]+[{**cols[2],'nullable':True}]+cols[3:]},
]:
    try:
        validate_native(broken)
        raise AssertionError('DDL drift accepted')
    except RuntimeError:
        pass
print('PASS V6: cross-database canonical SHA-256 + SQL Server drift guard')
