import ast
for f in ('main.py', 'data_sources.py'):
    ast.parse(open(f, encoding='utf-8').read())
    print(f, 'OK')

