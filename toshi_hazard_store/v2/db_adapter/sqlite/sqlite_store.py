"""
sqlite helpers to manage caching tables
"""
import base64
import json
import logging
import math
import pathlib
import sqlite3
from datetime import datetime as dt
from datetime import timezone
from typing import Generator, Iterable, List, Type, TypeVar, Union

import pynamodb.models
from pynamodb.attributes import JSONAttribute, ListAttribute
from pynamodb.expressions.condition import Condition
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER
from toshi_hazard_store.model.attributes import IMTValuesAttribute, LevelValuePairAttribute

# from pynamodb.attributes import ListAttribute, MapAttribute

TYPE_MAP = {"S": "string", "N": "numeric", "L": "string", "SS": "string"}

_T = TypeVar('_T', bound='pynamodb.models.Model')

log = logging.getLogger(__name__)


def get_hash_key(model_class):
    return model_class._hash_key_attribute().attr_name


def get_model(
    conn: sqlite3.Connection,
    model_class: Type[_T],
    hash_key: str,
    range_key_condition: Union[Condition, None] = None,
    filter_condition: Union[Condition, None] = None,
) -> Iterable[_T]:
    """query cache table and return any hits.

    :param conn: Connection object
    :param model_class: type of the model_class
    :return:
    """
    _sql = "SELECT * FROM %s \n" % safe_table_name(model_class)

    # first, the compulsory hash key
    _sql += f"\tWHERE {get_hash_key(model_class)}='{hash_key}'"

    # add the optional range_key_condition
    if range_key_condition is not None:
        _sql += "\n"
        for expr in sql_from_pynamodb_condition(range_key_condition):
            _sql += f"\tAND {expr}\n"

    # add the optional filter expression
    if filter_condition is not None:
        _sql += "\n"
        for expr in sql_from_pynamodb_condition(filter_condition):
            _sql += f"\tAND {expr}\n"

    log.debug(f"SQL: {_sql}")
    try:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(_sql):
            d = dict(row)
            for name, attr in model_class.get_attributes().items():

                # string conversion
                if attr.attr_type == 'S':
                    d[name] = str(d[name])

                # list conversion
                if attr.attr_type == 'L':
                    val = base64.b64decode(str(d[name])).decode('ascii')
                    d[name] = json.loads(val)
                    # TODO: this is only good for THS_HAZARDAGGREGATION
                    vals: List[Union[IMTValuesAttribute, LevelValuePairAttribute]] = list()
                    for itm in d[name]:
                        # print(itm)
                        log.debug(f"itm: {itm}")
                        if itm.get('M'):
                            m_itm = itm.get('M').get('imt')
                            if m_itm:
                                vals.append(
                                    IMTValuesAttribute(
                                        imt=m_itm.get('imt'),
                                        lvls=ListAttribute(m_itm.get('lvls')),
                                        vals=ListAttribute(m_itm.get('values')),
                                    )
                                )
                            else:
                                vals.append(LevelValuePairAttribute(lvl=itm['M']['lvl']['N'], val=itm['M']['val']['N']))
                        else:
                            raise ValueError("HUH")
                    d[name] = vals

                    # print('LIST:', name)
                    # print(d[name])

                # datetime conversion
                if isinstance(attr, TimestampAttribute):
                    d[name] = dt.fromtimestamp(d[name]).replace(tzinfo=timezone.utc)

            yield model_class(**d)
    except Exception as e:
        print(e)
        raise


def put_model(
    conn: sqlite3.Connection,
    model_instance: _T,
):
    """write model instance to query cache table.

    :param conn: Connection object
    :param model_instance: an instance the model_class
    :return: None
    """
    log.debug(f"model: {model_instance}")
    model_args = model_instance.get_save_kwargs_from_instance()['Item']

    _sql = "INSERT INTO %s \n" % safe_table_name(model_instance.__class__)  # model_class)
    _sql += "\t("
    # add attribute names
    for name in model_instance.get_attributes().keys():
        _sql += f'"{name}", '
    _sql = _sql[:-2] + ")\nVALUES (\n"

    # attrbute values
    for name, attr in model_instance.get_attributes().items():
        field = model_args.get(name)
        log.debug(f'attr {attr} {field}')

        if field is None:  # optional fields may not have been set, save `Null` instead
            _sql += '\tNull,\n'
            continue

        if isinstance(attr, JSONAttribute):
            b64_bytes = json.dumps(field["S"]).encode('ascii')
            _sql += f'\t"{base64.b64encode(b64_bytes).decode("ascii")}",\n'
            continue
        if field.get('SS'):  # SET
            b64_bytes = json.dumps(field["SS"]).encode('ascii')
            _sql += f'\t"{base64.b64encode(b64_bytes).decode("ascii")}",\n'
            continue
        if field.get('S'):  # String ir JSONstring
            _sql += f'\t"{field["S"]}",\n'
            continue
        if field.get('N'):
            _sql += f'\t{float(field["N"])},\n'
            continue
        if field.get('L'):  # LIST
            b64_bytes = json.dumps(field["L"]).encode('ascii')
            _sql += f'\t"{base64.b64encode(b64_bytes).decode("ascii")}",\n'
            continue
        raise ValueError("we should never get here....")
    _sql = _sql[:-2] + ");\n"

    log.debug('SQL: %s' % _sql)

    try:
        cursor = conn.cursor()
        cursor.execute(_sql)
        conn.commit()
        log.debug(f'cursor: {cursor}')
        log.debug("Last row id: %s" % cursor.lastrowid)
        # cursor.close()
        # conn.execute(_sql)
    except (sqlite3.IntegrityError) as e:
        msg = str(e)
        if 'UNIQUE constraint failed' in msg:
            log.info('attempt to insert a duplicate key failed: ')
    except Exception as e:
        log.error(e)
        raise


def cache_enabled() -> bool:
    """return Ture if the cache is correctly configured."""
    if LOCAL_CACHE_FOLDER is not None:
        if pathlib.Path(LOCAL_CACHE_FOLDER).exists():
            return True
        else:
            log.warning(f"Configured cache folder {LOCAL_CACHE_FOLDER} does not exist. Caching is disabled")
            return False
    else:
        log.warning("Local caching is disabled, please check config settings")
        return False


def get_connection(model_class: Type[_T]) -> sqlite3.Connection:
    if not cache_enabled():
        raise RuntimeError("cannot create connection ")
    log.info(f"get connection for {model_class}")
    return sqlite3.connect(pathlib.Path(str(LOCAL_CACHE_FOLDER), DEPLOYMENT_STAGE))


def safe_table_name(model_class: Type[_T]):
    return model_class.Meta.table_name.replace('-', '_')


def check_exists(conn: sqlite3.Connection, model_class: Type[_T]) -> bool:
    table_name = safe_table_name(model_class)
    sql = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"

    log.info(f"check_exists sql: {sql}")
    try:
        res = conn.execute(sql)
        table_found = next(res)[0] == table_name
    except StopIteration:
        table_found = False
    except Exception as e:
        log.error(str(e))
    return table_found


def drop_table(conn: sqlite3.Connection, model_class: Type[_T]) -> bool:
    table_name = safe_table_name(model_class)
    sql = f"DROP TABLE '{table_name}';"
    log.debug(f"drop table sql: {sql}")
    try:
        conn.execute(sql)
        return True
    except Exception as e:
        log.error(str(e))
        return False


def ensure_table_exists(conn: sqlite3.Connection, model_class: Type[_T]):
    """create if needed a cache table for the model_class
    :param conn: Connection object
    :param model_class: type of the model_class
    :return:
    """

    def create_table_sql(model_class: Type[_T]) -> str:

        # TEXT, NUMERIC, INTEGER, REAL, BLOB
        # print(name, _type, _type.attr_type)
        # print(dir(_type))
        _sql: str = "CREATE TABLE IF NOT EXISTS %s (\n" % safe_table_name(model_class)

        for name, attr in model_class.get_attributes().items():
            if attr.attr_type not in TYPE_MAP.keys():
                raise ValueError(f"Unupported type: {attr.attr_type} for attribute {attr.attr_name}")
            _sql += f'\t"{name}" {TYPE_MAP[attr.attr_type]},\n'

        # now add the primary key
        if model_class._range_key_attribute() and model_class._hash_key_attribute():
            return (
                _sql
                + f"\tPRIMARY KEY ({model_class._hash_key_attribute().attr_name}, "
                + f"{model_class._range_key_attribute().attr_name})\n)"
            )
        if model_class._hash_key_attribute():
            return _sql + f"\tPRIMARY KEY {model_class._hash_key_attribute().attr_name}\n)"
        raise ValueError()

    print('model_class', model_class)
    create_sql = create_table_sql(model_class)

    print(create_sql)
    try:
        conn.execute(create_sql)
    except Exception as e:
        print("EXCEPTION", e)


def execute_sql(conn: sqlite3.Connection, model_class: Type[_T], sql_statement: str):
    """
    :param conn: Connection object
    :param model_class: type of the model_class
    :return:
    """
    try:
        res = conn.execute(sql_statement)
    except Exception as e:
        print("EXCEPTION", e)
    return res


def _expand_expression(attr_type: str, expr: Iterable) -> Iterable[str]:
    if attr_type == 'N':
        return ", ".join([itm.value[attr_type] for itm in expr])
        # return ", ".join([str(float(itm.value[attr_type])) for itm in expr])
    if attr_type == 'S':
        return ", ".join([f'"{itm.value[attr_type]}"' for itm in expr])
    else:
        raise RuntimeError(f'{attr_type} not supported')


def _unpack_pynamodb_condition(condition: Condition) -> str:
    path = condition.values[0]
    expression = condition.values[1:]  # Union[Value, Condition], : Tuple[Any, ...]
    operator = condition.operator

    attr_name = path.attribute.attr_name
    attr_type = path.attribute.attr_type

    if operator == 'IN':
        return f'{attr_name} {operator} ({_expand_expression(attr_type, expression)})'

    # unary
    if len(condition.values[1:]) == 1:
        expr = condition.values[1]
    value = expr.value[attr_type]

    if attr_type == 'S':
        return f'{attr_name} {operator} "{value}"'
    if attr_type == 'N':
        return f'{attr_name} {operator} {value}'
    return f'{attr_name} {operator} {value}'


def sql_from_pynamodb_condition(condition: Condition) -> Generator:
    """build SQL expression from the pynamodb condition"""

    operator = condition.operator
    # handle nested condition
    if operator == 'AND':
        for cond in condition.values:
            for expr in sql_from_pynamodb_condition(cond):
                yield expr
    else:
        yield _unpack_pynamodb_condition(condition)


def _unpack_pynamodb_condition_count(condition: Condition) -> int:
    expression = condition.values[1:]
    operator = condition.operator
    if operator == 'IN':
        return len(expression)
    else:
        return 1


def _gen_count_permutations(condition: Condition) -> Iterable[int]:
    # return the number of hits expected, based on the filter conditin expression

    operator = condition.operator
    # handle nested
    count = 0
    if operator == 'AND':
        for cond in condition.values:
            for _count in _gen_count_permutations(cond):
                yield _count
    else:
        yield count + _unpack_pynamodb_condition_count(condition)


def count_permutations(condition: Condition) -> int:
    return math.prod(_gen_count_permutations(condition))
