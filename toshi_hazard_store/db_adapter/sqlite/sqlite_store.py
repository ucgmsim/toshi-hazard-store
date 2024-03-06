"""
sqlite helpers to manage caching tables
"""

import base64
import json
import logging
import pathlib
import pickle
import sqlite3
from typing import Iterable, List, Type, TypeVar, Union

import pynamodb.models
from nzshm_common.util import decompress_string
from pynamodb.expressions.condition import Condition

from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER

from .pynamodb_sql import SqlReadAdapter, SqlWriteAdapter, get_version_attribute, safe_table_name

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

    log.debug(sql)
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
    model_class = type(put_items[0])  # .__class__
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

    model_class = type(model_instance)
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

    if unique_failure:
        # try update query
        update_statement = swa.update_statement(model_instance)
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

    swa = SqlWriteAdapter(model_class)
    statement = swa.create_statement()

    log.debug(f'model_class {model_class}')
    log.debug(statement)

    try:
        conn.execute(statement)
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
