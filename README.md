# Arrow-mssql
[![PyPI](https://img.shields.io/pypi/v/arrow-mssql.svg)](https://pypi.org/project/arrow-mssql/)

## O que é o Arrow-mssql ?

É um projeto que recebe uma tabela ou consulta do `SQL SERVER`
e faz a exportação para um arquivo *.parquet* ou *.csv*,
utilizando a solução [arrow](https://arrow.apache.org/docs/index.html) que é uma tecnologia com
foco em análise e desempenho na memória.

Também é possível importar um arquivo *.parquet* para uma tabela do sql server ou arquivo *.csv*.

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
# EXPORTANDO UMA TABELA -- para csv
to_csv(
    DRIVER, 
    'NOME_TABELA',
    schema='dbo',
    database='seu_banco', 
    path='destino.csv'
)

# EXPORTANDO UMA CONSULTA -- para .parquet
to_parquet(
    DRIVER, 
    'SELECT N1, N2 FROM NOME_TABELA WHERE N1 = 0', 
    schema='dbo',
    database='seu_banco', 
    path='destino.parquet'
)
```

É possivel importar arquivos csv ou parquet, definir um limite de linhas
e colunas no processo.

```python
# IMPORTAR .parquet para tabela temporaria do ssms
# o retornor é um cursor referente a conexao com o banco de dados
with write_parquet(
    DRIVER, 
    '##teste', 
    path='origem.parquet'
) as C:

    ...

# AGORA é possivel importar um numero limite de linhas
# e selecionar as colunas no arquivo parquet
with write_parquet(
    DRIVER, 
    '##teste', 
    path='origem.parquet',
    limit=100,
    columns=['col1', 'col2']
) as C:

    ...
```