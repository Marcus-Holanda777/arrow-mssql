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

> somente a string connection é permitida


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

```python
```