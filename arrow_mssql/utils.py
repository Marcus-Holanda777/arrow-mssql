import sqlglot as sg
import sqlglot.expressions  as sge


def is_query(texto: str) -> bool:
    if sg.parse_one(texto, dialect='tsql').find(sge.Select):
        return True
    return False