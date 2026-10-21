"""Isolated pure policy checks: no Docker/psycopg needed."""
import ast
import hashlib
import json
import re
from pathlib import Path
source=(Path(__file__).resolve().parents[1]/'app/schema_evolution.py').read_text()
module=ast.parse(source)
functions={n.name:n for n in module.body if isinstance(n,ast.FunctionDef)}
needed=['stable','signature','diff','supported']
ns={'hashlib':hashlib,'json':json,'re':re,'MIGRATION_TYPE':'text'}
exec(compile(ast.Module(body=[functions[n] for n in needed],type_ignores=[]),'<policy>', 'exec'),ns)
base={'table':'public.contracts','primary_key':['id'],'columns':[
    {'name':'id','type':'bigint','nullable':False,'default':None,'identity':'','generated':''},
    {'name':'contract_code','type':'text','nullable':False,'default':None,'identity':'','generated':''},
    {'name':'amount','type':'numeric(14,2)','nullable':False,'default':None,'identity':'','generated':''},
    {'name':'status','type':'text','nullable':False,'default':None,'identity':'','generated':''},
    {'name':'description','type':'text','nullable':True,'default':None,'identity':'','generated':''},
]}
add={'name':'currency_code','type':'text','nullable':True,'default':None,'identity':'','generated':''}
import copy
new=copy.deepcopy(base);new['columns'].append(add)
assert ns['supported'](base,new)
assert ns['diff'](base,new)==[{'kind':'add_column','column':add}]
assert ns['signature'](base)==ns['signature'](copy.deepcopy(base))
for tamper in [
    {'nullable':False}, {'type':'integer'}, {'default':"'BRL'::text"},
    {'generated':'s'}, {'identity':'a'},
]:
    unsafe=copy.deepcopy(new)
    unsafe['columns'][-1].update(tamper)
    assert not ns['supported'](base,unsafe),tamper
renamed=copy.deepcopy(base);renamed['columns'][1]['name']='new_code'
assert not ns['supported'](base,renamed)
dropped=copy.deepcopy(base);dropped['columns'].pop()
assert not ns['supported'](base,dropped)
multiple=copy.deepcopy(new);multiple['columns'].append(add|{'name':'other'})
assert not ns['supported'](base,multiple)
pk=copy.deepcopy(base);pk['primary_key']=['contract_code']
assert not ns['supported'](base,pk)
index_change=copy.deepcopy(base)
index_change['indexes']=[{'name':'contracts_status_idx','definition':'CREATE INDEX contracts_status_idx ON public.contracts USING btree(status)'}]
assert not ns['supported'](base,index_change)
constraint_change=copy.deepcopy(base)
constraint_change['constraints']=[{'name':'contracts_positive','kind':'c','definition':'CHECK (amount >= 0)'}]
assert not ns['supported'](base,constraint_change)
print('PASS V5: schema policy, hashing, unsupported DDL rejection')
