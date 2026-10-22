"""Portable discovery, quoting, type safety and SELECT-only source contracts."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
import discovery_catalog as dc


class Cursor:
    def __init__(self, conn):
        self.conn=conn
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def execute(self, sql, args=()):
        self.conn.statements.append(sql)
        assert sql.startswith('SELECT '),sql
    def fetchall(self): return self.conn.rows
    def close(self): pass


class Connection:
    def __init__(self,rows): self.rows,self.statements=rows,[]
    def cursor(self,*args,**kwargs): return Cursor(self)


class CatalogTests(unittest.TestCase):
    def test_postgres_unknown_tables_and_dynamic_columns(self):
        rows=[('32','sales','new_table',True,'key','bigint',False,None,'','','',['key']),
              ('32','sales','new_table',True,'note','text',True,None,'','','C',['key'])]
        conn=Connection(rows)
        source=dc.Source('postgresql')
        first=source.inventory(conn)
        key=dc.table_key('postgresql','sales','new_table')
        self.assertEqual(first[key]['destination_schema'],'mirror_sales')
        self.assertEqual(first[key]['primary_key'],['key'])
        dc.validate_manifest(first[key])
        changed=deepcopy(rows)
        changed.append(('32','sales','new_table',True,'currency','character varying(3)',True,"'BRL'",'','','C',['key']))
        conn.rows=changed
        self.assertNotEqual(dc.signature(first),dc.signature(source.inventory(conn)))
        conn.rows=[(*rows[0][:5],'numeric(16,4)',*rows[0][6:]),rows[1]]
        self.assertNotEqual(dc.signature(first),dc.signature(source.inventory(conn)))
        conn.rows=[]
        self.assertEqual(source.inventory(conn),{})
        self.assertTrue(all(s.startswith('SELECT ') for s in conn.statements))

    def test_sqlserver_composite_keys_and_native_length_change(self):
        rows=[('dbo','new_table',7,'2026-01-01',1,'line_no','int',4,10,0,False,False,False,0,None,None,2),
              ('dbo','new_table',7,'2026-01-01',1,'order_id','bigint',8,19,0,False,False,False,0,None,None,1),
              ('dbo','new_table',7,'2026-01-01',1,'note','nvarchar',40,0,0,True,False,False,0,None,'Latin1_General_CI_AS',0)]
        conn=Connection(rows)
        source=dc.Source('sqlserver')
        key=dc.table_key('sqlserver','dbo','new_table')
        first=source.inventory(conn)[key]
        self.assertEqual(first['primary_key'],['order_id','line_no'])
        self.assertEqual(first['columns'][2]['target_type'],'character varying(20)')
        dc.validate_manifest(first)
        conn.rows=[*rows[:2],(*rows[2][:7],80,*rows[2][8:])]
        self.assertNotEqual(dc.signature(first),dc.signature(source.inventory(conn)[key]))

    def test_type_mapping_never_injects_sql_or_loses_seventh_fraction(self):
        self.assertEqual(dc.target_type('datetime2','sqlserver',scale=7),'text')
        self.assertEqual(dc.target_type('datetime2','sqlserver',scale=6),'timestamp')
        self.assertEqual(dc.target_type('numeric(16,4)','postgresql'),'numeric(16,4)')
        self.assertEqual(dc.target_type('text; DROP TABLE users','postgresql'),'text')
        manifest=dc.descriptor('postgresql','public','t','1',[
            {'name':'id','native_type':'bigint','target_type':'bigint'}],['id'])
        manifest['columns'][0]['target_type']='bigint; DROP TABLE users'
        with self.assertRaises(ValueError): dc.validate_manifest(manifest)

    def test_identifiers_are_quoted_and_long_names_do_not_collide(self):
        self.assertEqual(dc.identifier('a"; DROP TABLE x;--'),'"a""; DROP TABLE x;--"')
        self.assertEqual(dc.identifier('a]b','sqlserver'),'[a]]b]')
        for name in ('á'*100,'a'*100):
            self.assertLessEqual(len(dc.destination_name(name).encode()),63)
            self.assertNotEqual(dc.destination_name(name),dc.destination_name(name+'b'))
        with self.assertRaises(ValueError): dc.identifier('a\x00b')

    def test_select_permission_and_scope_are_part_of_discovery(self):
        rows=[('32','sales','private_table',False,'id','bigint',False,None,'','','',['id'])]
        source=dc.Source('postgresql')
        self.assertFalse(next(iter(source.inventory(Connection(rows)).values()))['readable'])
        with patch.dict('os.environ',{'SOURCE_SCHEMAS':'public'}):
            self.assertEqual(source.inventory(Connection(rows)),{})
        with self.assertRaisesRegex(ValueError,'must be SELECT'):
            source.query(Connection([]),'DELETE FROM source')


if __name__=='__main__': unittest.main()
