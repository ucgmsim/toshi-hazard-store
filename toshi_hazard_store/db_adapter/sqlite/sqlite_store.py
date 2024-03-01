"""
sqlite helpers to manage caching tables
"""

import base64
import json
import pickle
import logging
import pathlib
import sqlite3
from datetime import datetime as dt
from datetime import timezone
from typing import Generator, Iterable, List, Type, TypeVar, Union

import pynamodb.models
from pynamodb.attributes import JSONAttribute, ListAttribute, VersionAttribute
from pynamodb.expressions.condition import Condition
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER
from toshi_hazard_store.model.attributes import IMTValuesAttribute, LevelValuePairAttribute

# from pynamodb.attributes import ListAttribute, MapAttribute

# TYPE_MAP = {"S": "string", "N": "numeric", "L": "string", "SS": "string"}
# TYPE_MAP = {"S": "string", "N": "string", "L": "string", "SS": "string"}

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
    # val = f"{{'S': '{hash_key}'}}"
    # log.info(val)
    # assert 0
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

            # log.info(f"ROW as dict: {d}")
            # m = model_class().from_dynamodb_dict(d)
            # log.info(m)

            for name, attr in model_class.get_attributes().items():

                log.debug(f"DESERIALIZE: {name} {attr}")
                log.debug(f"{d[name]}, {type(d[name])}")
                if d[name]:
                    if attr.is_hash_key or attr.is_range_key:
                        continue
                    upk = pickle.loads(base64.b64decode(d[name]))
                    log.debug(upk)
                    log.debug(f"{attr.attr_name} {attr.attr_type} {upk} {type(upk)}")
                    # log.debug(f"{attr.get_value(upk)}")
                    if isinstance(upk, float):
                        d[name] = upk
                    else:
                        d[name] = attr.deserialize(upk)

            yield model_class(**d)
    except Exception as e:
        print(e)
        raise


def _attribute_value(simple_serialized, attr):

    value = simple_serialized.get(attr.attr_name)

    if value is None:
        return

    if attr.is_hash_key or attr.is_range_key:
        return value

    pkld = pickle.dumps(value)
    return base64.b64encode(pkld).decode('ascii')

def _attribute_values(model_instance: _T, exclude=None) -> str:
    # model_args = model_instance.get_save_kwargs_from_instance()['Item']
    _sql = ""

    exclude = exclude or []

    simple_serialized = model_instance.to_simple_dict(force=True)
    pynamodb_serialized = model_instance.to_dynamodb_dict()

    log.debug(f'SMP-SER: {simple_serialized}')
    log.debug(f'DYN-SER: {pynamodb_serialized}')
    for name, attr in model_instance.get_attributes().items():
        log.debug(f'attr {attr} {name}')

        if attr in exclude:
            continue

        # if attr.is_hash_key or attr.is_range_key:
        #     _sql += f'"{getattr(model_instance, name)}", '
        #     continue

        # log.debug(f"PYN-PKL {pynamodb_serialized.get(name)}")
        # log.debug(f"SMP-PKL {simple_serialized.get(name)}")

        # value = simple_serialized.get(name)
        # pkld = pickle.dumps(value)
        # sqlsafe = base64.b64encode(pkld).decode('ascii')

        value = _attribute_value(simple_serialized, attr)
        # assert v == sqlsafe
        _sql += f'"{value}", ' if value else 'NULL, '

    log.debug(_sql)
    return _sql[:-2]


def put_models(
    conn: sqlite3.Connection,
    put_items: List[_T],
):
    log.debug("put_models")

    _sql = "INSERT INTO %s \n" % safe_table_name(put_items[0].__class__)  # model_class)
    _sql += "("

    # add attribute names, taking first model
    for name in put_items[0].get_attributes().keys():
        _sql += f'"{name}", '
    _sql = _sql[:-2]
    _sql += ")\nVALUES \n"

    # if we have duplicates by primary key, take only the last value
    model_class = put_items[0].__class__
    if model_class._range_key_attribute() and model_class._hash_key_attribute():
        unique_on = [model_class._hash_key_attribute(), model_class._range_key_attribute()]
    else:
        unique_on = [model_class._hash_key_attribute()]

    unique_put_items = {}
    for model_instance in put_items:
        simple_serialized = model_instance.to_simple_dict(force=True)
        # model_args = model_instance.get_save_kwargs_from_instance()['Item']
        uniq_key = ":".join([f'{_attribute_value(simple_serialized, attr)}' for attr in unique_on])
        unique_put_items[uniq_key] = model_instance

    for item in unique_put_items.values():
        _sql += "\t(" + _attribute_values(item) + "),\n"

    _sql = _sql[:-2] + ";"

    log.info('SQL: %s' % _sql)

    try:
        cursor = conn.cursor()
        cursor.execute(_sql)
        conn.commit()
        log.debug(f'cursor: {cursor}')
        log.debug("Last row id: %s" % cursor.lastrowid)
        # cursor.close()
        # conn.execute(_sql)
    except sqlite3.IntegrityError as e:
        msg = str(e)
        if 'UNIQUE constraint failed' in msg:
            log.info('attempt to insert a duplicate key failed: ')
    except Exception as e:
        log.error(e)
        raise


# def _get_sql_field_value(model_args, value):
#     field = model_args.get(value.attr_name)

#     log.debug(f'_get_sql_field_value: {value} {field}')

#     # log.debug(f"serialize: {value.serialize(value)}")
#     # assert 0

#     if field is None:  # optional fields may not have been set, save `Null` instead
#         return 'Null'

#     if isinstance(value, JSONAttribute):
#         b64_bytes = json.dumps(field["S"]).encode('ascii')
#         return f'"{base64.b64encode(b64_bytes).decode("ascii")}"'

#     if field.get('SS'):  # SET
#         b64_bytes = json.dumps(field["SS"]).encode('ascii')
#         return f'"{base64.b64encode(b64_bytes).decode("ascii")}"'

#     if field.get('S'):  # String or JSONstring
#         return f'"{field["S"]}"'

#     if field.get('N'):
#         return f'{float(field["N"])}'

#     if field.get('L'):  # LIST
#         b64_bytes = json.dumps(field["L"]).encode('ascii')
#         return f'"{base64.b64encode(b64_bytes).decode("ascii")}"'

#     # handle empty string field
#     if field.get('S') == "":
#         return '""'


def _get_version_attribute(model_instance: _T):
    for name, value in model_instance.get_attributes().items():
        if isinstance(value, VersionAttribute):
            return value


def _insert_into_sql(model_instance: _T):
    _sql = "INSERT INTO %s \n" % safe_table_name(model_instance.__class__)  # model_class)
    _sql += "\t("
    # add attribute names
    # log.debug(dir(model_instance))
    # assert 0

    for name, value in model_instance.get_attributes().items():
        _sql += f'"{name}", '
    _sql = _sql[:-2] + ")\nVALUES ("
    _sql += _attribute_values(model_instance) + ");\n"
    log.debug('SQL: %s' % _sql)
    return _sql


def _update_sql(
    model_instance: _T,
):
    key_fields = []

    simple_serialized = model_instance.to_simple_dict(force=True)

    _sql = "UPDATE %s \n" % safe_table_name(model_instance.__class__)  # model_class)
    _sql += "SET "

    # add non-key attribute pairs
    for name, attr in model_instance.get_attributes().items():
        if attr.is_hash_key or attr.is_range_key:
            key_fields.append(attr)
            continue
        value = _attribute_value(simple_serialized, attr)
        if value:
            _sql += f'\t{name} = "{value}", \n'
        else:
            _sql += f'\t{name} = NULL, \n'

    _sql = _sql[:-3] + "\n"

    _sql += "WHERE "

    for attr in key_fields:
        #field = simple.get(item.attr_name)
        #print(field)
        _sql += f'\t{attr.attr_name} = "{_attribute_value(simple_serialized, attr)}" AND\n'

    version_attr = _get_version_attribute(model_instance)
    if version_attr:
        # add constraint
        _sql += f'\t{version_attr.attr_name} = {int(float(_attribute_value(simple_serialized, version_attr))-1)};\n'
    else:
        _sql = _sql[:-4] + ";\n"
    log.debug('SQL: %s' % _sql)
    return _sql


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
    unique_failure = False
    try:
        cursor = conn.cursor()
        cursor.execute(_insert_into_sql(model_instance))
        conn.commit()
        log.debug(f'cursor: {cursor}')
        log.debug("Last row id: %s" % cursor.lastrowid)
        # cursor.close()
        # conn.execute(_sql)
    except sqlite3.IntegrityError as e:
        msg = str(e)
        if 'UNIQUE constraint failed' in msg:
            log.info('attempt to insert a duplicate key failed: ')
        unique_failure = True
        version_attr = _get_version_attribute(model_instance)
        if version_attr:
            raise
    except Exception as e:
        log.error(e)
        raise

    if unique_failure:
        # try update query
        cursor = conn.cursor()
        cursor.execute(_update_sql(model_instance))
        conn.commit()
        log.debug(f'cursor: {cursor}')
        log.debug("Last row id: %s" % cursor.lastrowid)


def get_connection(model_class: Type[_T]) -> sqlite3.Connection:
    log.info(f"get connection for {model_class} using path {LOCAL_CACHE_FOLDER}/{DEPLOYMENT_STAGE}")
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
            # if attr.attr_type not in TYPE_MAP.keys():
            #     raise ValueError(f"Unupported type: {attr.attr_type} for attribute {attr.attr_name}")
            _sql += f'\t"{name}" string,\n'

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

    log.debug(f'model_class {model_class}')
    create_sql = create_table_sql(model_class)

    log.debug(create_sql)

    try:
        conn.execute(create_sql)
    except Exception as e:
        print("EXCEPTION", e)
        raise

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
