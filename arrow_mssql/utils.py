import sqlglot as sg
import sqlglot.expressions  as sge
import unicodedata


def is_query(texto: str) -> bool:
    if sg.parse_one(texto, dialect='tsql').find(sge.Select):
        return True
    return False


def remove_accet(txt: str) -> str:
    t_01 = unicodedata.normalize('NFD', txt)
    t_02 = ''.join(c for c in t_01 if not unicodedata.combining(c))
    return unicodedata.normalize('NFC', t_02)


def remove_especial(text: str) -> str:
    import re
    return re.sub(r'[^a-zA-Z0-9 _]', '', text)


def rename_col(col: str) -> str:
    return '_'.join(
        remove_especial(
            remove_accet(c.strip())
        )
        .casefold() 
        for c in col.split()
    )