"""Read-only HTML presentation of persisted schema and execution evidence."""
import html
import json
from urllib.parse import urlencode


def safe(value):
    return html.escape('—' if value is None else str(value),quote=True)


def modal_link(url, label, code=False):
    content=safe(label)
    if code:
        content='<code>'+content+'</code>'
    return f'<a href="{safe(url)}" data-modal aria-haspopup="dialog">{content}</a>'


def table_link(item, label=None):
    label=label or item.get('object') or item.get('source') or item.get('table') or 'Tabela'
    if item.get('key') or item.get('table_key'):
        query={'table_key':item.get('key') or item['table_key']}
    elif item.get('source_key'):
        query={'source_key':item['source_key']}
    elif item.get('id'):
        query={'run_id':item['id']}
    else:
        return '<code>'+safe(label)+'</code>'
    return modal_link('/ddl?'+urlencode(query),label,code=True)


def quote_identifier(name, engine):
    return '['+name.replace(']',']]')+']' if engine=='sqlserver' else '"'+name.replace('"','""')+'"'


def native_type(column, engine):
    value=column.get('native_type') or column.get('type') or '/* tipo não registrado */'
    if engine!='sqlserver' or '(' in value:
        return value
    name=value.lower()
    length=column.get('length')
    if length is not None and name in ('varchar','nvarchar','char','nchar','binary','varbinary'):
        size='MAX' if length==-1 else str(length//2 if name.startswith('n') else length)
        return f'{value}({size})'
    if name in ('numeric','decimal') and column.get('precision') is not None:
        return f'{value}({column["precision"]},{column.get("scale",0)})'
    if name in ('datetime2','datetimeoffset','time') and column.get('scale') is not None:
        return f'{value}({column["scale"]})'
    return value


def source_ddl(manifest):
    """A reconstruction, never an executable migration or an exact schema dump."""
    engine=manifest.get('engine','postgresql')
    table=manifest.get('table','tabela')
    schema=manifest.get('schema')
    if not schema:
        schema,_,table=table.partition('.') if '.' in table else (('dbo' if engine=='sqlserver' else 'public'),'.',table)
    quote=lambda value:quote_identifier(value,engine)
    columns=[]
    for column in manifest.get('columns',[]):
        name=quote(column['name'])
        expression=column.get('computed_expression')
        default=column.get('default',column.get('default_expression'))
        if engine=='sqlserver' and column.get('computed') and expression:
            definition=f'{name} AS ({expression})'
        else:
            definition=f'{name} {native_type(column,engine)}'
            if engine=='postgresql' and column.get('generated') and default is not None:
                storage='VIRTUAL' if column['generated']=='v' else 'STORED'
                definition+=f' GENERATED ALWAYS AS ({default}) {storage}'
            elif engine=='postgresql' and column.get('identity') in ('a','d'):
                mode='ALWAYS' if column['identity']=='a' else 'BY DEFAULT'
                definition+=f' GENERATED {mode} AS IDENTITY'
            elif default is not None:
                definition+=' DEFAULT '+str(default)
            if column.get('nullable') is not None:
                definition+=' NULL' if column['nullable'] else ' NOT NULL'
        notes=[]
        if engine=='sqlserver' and column.get('identity'):
            notes.append('IDENTITY: seed e incremento não registrados')
        if column.get('computed') and not expression:
            notes.append('coluna calculada: expressão não registrada')
        if column.get('default_present') and default is None:
            notes.append('DEFAULT existente: expressão sem visibilidade')
        if notes:
            definition+=' /* '+'; '.join(notes)+' */'
        columns.append('    '+definition)
    if manifest.get('primary_key'):
        columns.append('    PRIMARY KEY ('+', '.join(quote(name) for name in manifest['primary_key'])+')')
    return 'CREATE TABLE '+quote(schema)+'.'+quote(table)+' (\n'+',\n'.join(columns)+'\n);'


def ddl_body(record, date_time):
    manifest=record['manifest']
    engine={'postgresql':'PostgreSQL','sqlserver':'SQL Server'}.get(manifest.get('engine'),'PostgreSQL')
    context='Estrutura registrada nesta execução' if record.get('run_id') else 'Última estrutura observada'
    body=f'<p class="detail-note">{safe(context)} · {safe(engine)}</p>'
    if record.get('observed_at'):
        body+='<p class="detail-note">Registrada em '+date_time(record['observed_at'])+'</p>'
    if record.get('status'):
        body+='<p class="detail-note">Estado registrado: <code>'+safe(record['status'])+'</code></p>'
    body+='<pre class="ddl-code"><code>'+safe(source_ddl(manifest))+'</code></pre>'
    body+='<p class="detail-note">DDL reconstruído a partir do catálogo local. Não é uma cópia completa do schema: índices, FKs, triggers e propriedades não observadas podem estar ausentes. Nenhum comando é executado na origem.</p>'
    return body


LABELS={'run_id':'ID da execução','engine':'Motor da origem','source_engine':'Motor da origem',
        'schema':'Schema da origem','table':'Tabela da origem','mode':'Modo','status':'Estado',
        'base_generation':'Geração base','result_generation':'Geração publicada',
        'full_snapshot':'Cópia completa','delta_count':'Payloads novos ou alterados',
        'seen_count':'Linhas conferidas','sweep_count':'Linhas conferidas',
        'inserted_count':'INSERT','updated_count':'UPDATE','deleted_count':'DELETE',
        'logical_bytes':'Bytes JSON','logical_kafka_bytes':'Bytes JSON','error':'Erro',
        'started_at':'Início','finished_at':'Conclusão','source_cutoff':'Limite temporal da leitura',
        'data_schema_hash':'Hash do contrato','sweep_low':'Início da faixa','sweep_high':'Fim da faixa',
        'sweep_target_max_id':'Limite superior observado','result_version':'Geração publicada'}


def run_body(data, date_time):
    rows=[]
    for key,value in data.items():
        if key=='manifest':
            continue
        if key in ('started_at','finished_at','source_cutoff'):
            rendered=date_time(value)
        elif key=='table':
            rendered=table_link({'id':data['run_id']},value)
        else:
            rendered='<code>'+safe(value)+'</code>'
        rows.append('<tr><th scope="row">'+safe(LABELS.get(key,key))+'</th><td>'+rendered+'</td></tr>')
    body='<p>'+modal_link('/ddl?'+urlencode({'run_id':data['run_id']}),'Ver DDL registrado para esta execução')+'</p>'
    body+='<div class="data"><table class="detail-table">'+''.join(rows)+'</table></div>'
    if data.get('manifest'):
        body+='<details class="technical"><summary>Catálogo registrado no lote</summary><pre class="ddl-code"><code>'+safe(json.dumps(data['manifest'],ensure_ascii=False,indent=2))+'</code></pre></details>'
    return body


DETAIL_CSS='''
.detail-note{color:#657589;line-height:1.6;font-size:13px}
.ddl-code{background:#f5f7fa;border:1px solid #e1e6ed;border-radius:9px;padding:16px;overflow:auto;white-space:pre;line-height:1.7;font:12px ui-monospace,monospace}
.detail-table{min-width:0;table-layout:fixed}.detail-table th{width:36%}.detail-table td{overflow-wrap:anywhere}
'''


def detail_page(title, body):
    return ('<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>'+safe(title)+'</title><style>body{font:14px system-ui;color:#182638;max-width:980px;margin:30px auto;padding:0 20px}'
            'a{color:#215ca6}table{width:100%;border-collapse:collapse;text-align:left}th,td{padding:10px;border-bottom:1px solid #e1e6ed}'
            +DETAIL_CSS+'</style></head><body><p><a href="/">← Painel</a></p><h1>'+safe(title)+'</h1>'+body+'</body></html>')
