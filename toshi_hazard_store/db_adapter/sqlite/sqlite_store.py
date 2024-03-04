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

from nzshm_common.util import compress_string, decompress_string

import pynamodb.models
from pynamodb.attributes import JSONAttribute, ListAttribute, VersionAttribute
from pynamodb.expressions.condition import Condition
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER
from toshi_hazard_store.model.attributes import IMTValuesAttribute, LevelValuePairAttribute

from .pynamodb_sql import (
    safe_table_name,
    sql_from_pynamodb_condition,
    get_version_attribute,
    get_hash_key,
    SqlWriteAdapter,
    SqlReadAdapter,
)

_T = TypeVar('_T', bound='pynamodb.models.Model')

log = logging.getLogger(__name__)


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

    sra = SqlReadAdapter(model_class)
    sql = sra.query_statement(hash_key, range_key_condition, filter_condition)

    # TODO: push this conversion into the SqlReadAdapter class
    try:
        conn.row_factory = sqlite3.Row
        for row in conn.execute(sql):
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

                    # if attr.__class__ == pynamodb.attributes.UnicodeAttribute:
                    #     continue

                    if type(attr) == pynamodb.attributes.JSONAttribute:
                        d[name] = json.loads(decompress_string(d[name]))
                        continue

                    try:
                        # May not pickled, maybe just standard serialisation
                        d[name] = pickle.loads(base64.b64decode(d[name]))
                        log.debug(d[name])
                        # log.debug(f"{attr.attr_name} {attr.attr_type} {upk} {type(upk)}")

                        # if isinstance(upk, float):
                        #     d[name] = upk
                        # else:
                        #     d[name] = attr.deserialize(upk)
                        # continue
                    except Exception as exc:
                        log.debug(f"{attr.attr_name} {attr.attr_type} {exc}")

                    try:
                        # maybe not serialized
                        d[name] = attr.deserialize(attr.get_value(d[name]))
                        continue
                    except Exception as exc:
                        log.debug(f"{attr.attr_name} {attr.attr_type} {exc}")

                    # Dont do anything
                    continue

                    # if "pynamodb_attributes.timestamp.TimestampAttribute" in str(attr):
                    #     log.debug(attr.attr_type)
                    #     log.debug(attr.attr_path)
                    #     log.debug(attr.__class__)
                    #     log.debug(attr.deserialize(upk))
                    #     assert 0

                    # log.debug(f"{attr.get_value(upk)}")
                    # try to deserialize
                    # try:
                    #     d[name] = attr.deserialize(upk)
                    #     continue
                    # except (Exception):
                    #     pass

                    # if isinstance(upk, float):
                    #     d[name] = upk
                    # else:
                    #     d[name] = upk #

            log.debug(f"d {d}")

            # yield model_class().from_simple_dict(d)
            yield model_class(**d)

    except Exception as e:
        print(e)
        raise


def put_models(
    conn: sqlite3.Connection,
    put_items: List[_T],
):
    model_class = put_items[0].__class__
    swa = SqlWriteAdapter(model_class)
    swa.insert_into(conn, put_items)


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

    model_class = model_instance.__class__
    swa = SqlWriteAdapter(model_class)
    statement = swa.insert_statement([model_instance])

    # swa.insert_into(conn, put_items)
    # custom error handling follows
    try:
        cursor = conn.cursor()
        cursor.execute(statement)
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
        version_attr = get_version_attribute(model_instance)
        if version_attr:
            raise
    except Exception as e:
        log.debug(f'SQL: {statement}')
        log.error(e)
        raise

    update_statement = swa.update_statement(model_instance)

    if unique_failure:
        # try update query
        cursor = conn.cursor()
        cursor.execute(update_statement)
        conn.commit()
        log.debug(f'cursor: {cursor}')
        log.debug("Last row id: %s" % cursor.lastrowid)


def get_connection(model_class: Type[_T]) -> sqlite3.Connection:
    log.info(f"get connection for {model_class} using path {LOCAL_CACHE_FOLDER}/{DEPLOYMENT_STAGE}")
    return sqlite3.connect(pathlib.Path(str(LOCAL_CACHE_FOLDER), DEPLOYMENT_STAGE))


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
