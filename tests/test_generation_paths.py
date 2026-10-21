"""Pure Python tests for the ancestry logic (doesn't need psycopg or Docker)."""
import ast
from pathlib import Path

source = (Path(__file__).resolve().parents[1]/'app/lab.py').read_text()
module = ast.parse(source)
selected = ['ancestry', 'align_standby']
subtree = ast.Module(body=[node for node in module.body
                           if isinstance(node,ast.FunctionDef) and node.name in selected],
                     type_ignores=[])
namespace = {}
exec(compile(ast.fix_missing_locations(subtree),'<lab-logic>','exec'), namespace)
ops = []

class Result:
    def __init__(self, value): self.value=value
    def fetchone(self): return self.value

class FakeConnection:
    def __init__(self): self.parents={1:None, 2:1, 3:2, 4:2}
    def execute(self, sql, args=None):
        if sql.startswith('SELECT parent_version'):
            return Result((self.parents[args[0]],))
        if sql.startswith('UPDATE replica.slot_state'):
            ops.append(('set-version',args[0],args[1]))
            return Result(None)
        raise AssertionError(sql)

conn=FakeConnection()
namespace['replay_journal']=lambda c,v,slot,field: ops.append((field,v,slot))
namespace['log']=lambda *args,**kwargs: None
namespace['align_standby'](conn,'blue',1,3)
assert ops == [('after_row',2,'blue'),('after_row',3,'blue'),('set-version',3,'blue')],ops
ops.clear()
namespace['align_standby'](conn,'green',3,4)
assert ops == [('before_row',3,'green'),('after_row',4,'green'),('set-version',4,'green')],ops
ops.clear()
namespace['align_standby'](conn,'blue',4,2)
assert ops == [('before_row',4,'blue'),('set-version',2,'blue')],ops
print('PASS: blue-green generation paths, rollback branches, forward/inverse replay')
