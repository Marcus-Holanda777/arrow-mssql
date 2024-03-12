# Arrow-mssql

## O que é o Arrow-mssql ?

é um projeto que recebe uma tabela ou consulta do `SQL SERVER`
e faz a exportação para um arquivo *.parquet* ou *.csv*,
utilizando a solução [arrow](https://arrow.apache.org/docs/index.html) que é uma tecnologia com
foco em análise e desempenho na memória.

## Instalação

```bash
pip install arrow-mssql
```

## Conexão

Para se conectar ao sql server o driver padrão
é o `pyodbc` é preciso fornecedor uma string de conexao

> somente a string de conexão é permitida


```python
DRIVER = (
    'Driver={ODBC Driver 18 for Sql Server};'
    'Server=seu_servidor;'
    'Database=seu_banco_de_dados;'
    'TrustServerCertificate=Yes;'
    'Authentication=ActiveDirectoryIntegrated;'
)
```

## Como usar ?

Tanto uma tabela ou consulta pode ser exportada

> a exportacao é feita de forma incrimental por lotes de dados

```python
# EXPORTANDO UMA TABELA
to_parquet(
    DRIVER, 
    'NOME_TABELA',
    schema='dbo',
    database='seu_banco', 
    path='destino.parquet'
)

# EXPORTANDO UMA CONSULTA
to_parquet(
    DRIVER, 
    'SELECT N1, N2 FROM NOME_TABELA WHERE N1 = 0', 
    schema='dbo',
    database='seu_banco', 
    path='destino.parquet'
    query=True
)

```