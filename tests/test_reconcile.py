"""Pure-function test for complete-range hash comparisons."""
import ast
from pathlib import Path
src=(Path(__file__).resolve().parents[1]/'app/lab.py').read_text()
module=ast.parse(src)
funcs=[x for x in module.body if isinstance(x,ast.FunctionDef) and x.name=='diff_keys']
ns={}
exec(compile(ast.fix_missing_locations(ast.Module(body=funcs,type_ignores=[])), '<extracted>', 'exec'), ns)
diff=ns['diff_keys']
changed, deleted=diff([(1,'a'),(2,'B'),(4,'d')],[(1,'a'),(2,'b'),(3,'c')])
assert changed==[2,4],(changed,deleted)
assert deleted==[3],(changed,deleted)
assert diff([],[(12,'v')])==([], [12])
assert diff([(2,'same')],[(2,'same')])==([], [])
print('PASS V4: changed / new / absent IDs')
