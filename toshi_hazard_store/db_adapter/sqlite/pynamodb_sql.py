"""pynamodb_sql.py

A class to handle storing/retrieving pynamodb models into sqlite3

 - take a pynamodb model instance (6.0.0)
    - [X] serialise / deserialise basic attributes so they're queryable
    - custom attributes MAY be queryable (configuration) TODO make this more that a list QUERY_ARG_ATTRIBUTES
    - extended attributes from pynamodo_attributes Timestamp etc

 - buld valid SQL for
    - [x] CREATE TABLE
    - [x] UPDATE WHERE
    - [x] INSERT INTO
    - [x] SELECT WHERE

 - Queries must produce original Pynamodb Model
 - Queries should support types for querying on STRING and Numeric fields but NOT on lists, custom / complex types
"""

import base64
import json
import logging
import pickle
import sqlite3
from typing import Generator, Iterable, List, Type, TypeVar, Union

import pynamodb.models
from nzshm_common.util import compress_string
from pynamodb.attributes import VersionAttribute

# from pynamodb.constants import DELETE, PUT
from pynamodb.expressions.condition import Condition
from pynamodb_attributes import IntegerAttribute

# import toshi_hazard_store.model.attributes
from toshi_hazard_store.model.attributes import (
    EnumConstrainedIntegerAttribute,
    EnumConstrainedUnicodeAttribute,
    ForeignKeyAttribute,
)

_T = TypeVar('_T', bound='pynamodb.models.Model')

log = logging.getLogger(__name__)

QUERY_ARG_ATTRIBUTES = [
    pynamodb.attributes.UnicodeAttribute,
    # pynamodb.attributes.VersionAttribute,
    pynamodb.attributes.NumberAttribute,
    EnumConstrainedUnicodeAttribute,
    EnumConstrainedIntegerAttribute,
    IntegerAttribute,
    ForeignKeyAttribute,
]


def safe_table_name(model_class: Type[_T]):
    """Get a sql-safe table name from the model_class"""
    return model_class.Meta.table_name.replace('-', '_')


def get_hash_key(model_class: Type[_T]):
    return model_class._hash_key_attribute().attr_name


def get_version_attribute(model_instance: _T):
    for name, value in model_instance.get_attributes().items():
        if isinstance(value, VersionAttribute):
            return value


class SqlReadAdapter:

    def __init__(self, model_class: Type[_T]):
        self.model_class = model_class

    def count_statement(
        self,
        hash_key: str,
        range_key_condition: Union[Condition, None] = None,
        filter_condition: Union[Condition, None] = None,
    ) -> str:
        """Build a SQL `SELECT COUNT(*) ...` statement"""
        _sql = self.query_statement(hash_key, range_key_condition, filter_condition)
        _sql = _sql.replace("SELECT *", "SELECT count(*)")
        log.debug(_sql)
        return _sql

    def query_statement(
        self,
        hash_key: str,
        range_key_condition: Union[Condition, None] = None,
        filter_condition: Union[Condition, None] = None,
    ) -> str:
        """Build a `SQL SELECT ...` statement"""

        _sql = "SELECT * FROM %s \n" % safe_table_name(self.model_class)
        _sql += f"\tWHERE {get_hash_key(self.model_class)}='{hash_key}'"

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

        log.debug(_sql)
        return _sql


class SqlWriteAdapter:

    def __init__(self, model_class: Type[_T]):
        self.model_class = model_class

    def _attribute_value(self, model_instance, attr):
        """Take a pynamodb serialized dict

        and return the form to be stored to SQL"""

        value = getattr(model_instance, attr.attr_name)
        if value is None:
            return

        if attr.is_hash_key or attr.is_range_key:
            return value

        if type(attr) == pynamodb.attributes.JSONAttribute:
            log.debug("compressing JSONAttribute type {attr.attr_name}")
            return compress_string(json.dumps(value))

        if type(attr) in [pynamodb.attributes.ListAttribute, pynamodb.attributes.UnicodeSetAttribute]:
            pkld = pickle.dumps(value)
            log.debug(f"pickling {attr.attr_name} of {type(attr)} containing {value}")
            return base64.b64encode(pkld).decode('ascii')
        # for query_arg_type in QUERY_ARG_ATTRIBUTES:
        #     # type(attr) == query_arg_type
        #     if isinstance(attr, query_arg_type):
        #         return value

        return attr.serialize(value)

    def _attribute_values(self, model_instance, exclude=None) -> str:
        _sql = ""
        exclude = exclude or []
        for name, attr in model_instance.get_attributes().items():
            if attr in exclude:
                continue

            value = self._attribute_value(model_instance, attr)
            if value is None:
                _sql += 'NULL, '
            else:
                _sql += f'"{value}", '

        log.debug(_sql)
        return _sql[:-2]

    def create_statement(self) -> str:

        # TEXT, NUMERIC, INTEGER, REAL, BLOB
        # print(name, _type, _type.attr_type)
        # print(dir(_type))
        _sql: str = "CREATE TABLE IF NOT EXISTS %s (\n" % safe_table_name(self.model_class)
        # version_attr = None
        for name, attr in self.model_class.get_attributes().items():
            # if attr.attr_type not in TYPE_MAP.keys():
            #     raise ValueError(f"Unupported type: {attr.attr_type} for attribute {attr.attr_name}")
            field_type = 'NUMERIC' if attr.attr_type == 'N' else 'STRING'
            _sql += f'\t"{attr.attr_name}" {field_type},\n'
            # print(name, attr, attr.attr_name, attr.attr_type)
            # if isinstance(attr, VersionAttribute):
            #     version_attr = attr

        # now add the primary key
        if self.model_class._range_key_attribute() and self.model_class._hash_key_attribute():
            return (
                _sql
                + f"\tPRIMARY KEY ({self.model_class._hash_key_attribute().attr_name}, "
                + f"{self.model_class._range_key_attribute().attr_name})\n)"
            )
        if self.model_class._hash_key_attribute():
            return _sql + f"\tPRIMARY KEY {self.model_class._hash_key_attribute().attr_name}\n)"
        raise ValueError()

    def update_statement(
        self,
        model_instance: _T,
    ) -> str:
        key_fields = []
        _sql = "UPDATE %s \n" % safe_table_name(model_instance.__class__)  # model_class)
        _sql += "SET "

        # add non-key attribute pairs
        for name, attr in model_instance.get_attributes().items():
            if attr.is_hash_key or attr.is_range_key:
                key_fields.append(attr)
                continue
            value = self._attribute_value(model_instance, attr)
            if value is None:
                _sql += f'\t"{attr.attr_name}" = NULL, \n'
            elif attr.attr_type == 'N':
                _sql += f'\t"{attr.attr_name}" = {value}, \n'
            else:
                _sql += f'\t"{attr.attr_name}" = "{value}", \n'

        _sql = _sql[:-3] + "\n"

        _sql += "WHERE "

        for attr in key_fields:
            # field = simple.get(item.attr_name)
            # print(field)
            _sql += f'\t{attr.attr_name} = "{self._attribute_value(model_instance, attr)}" AND\n'

        version_attr = get_version_attribute(model_instance)
        if version_attr:
            # add constraint
            version = self._attribute_value(model_instance, version_attr)
            _sql += f'\t{version_attr.attr_name} = {int(version)-1}\n'
        else:
            _sql = _sql[:-4]
        _sql += ";"
        log.debug('SQL: %s' % _sql)
        return _sql

    def insert_statement(self, put_items: List[_T]) -> str:
        """Build a valid INSERT INTO SQL statement"""

        log.debug("put_models")

        _sql = "INSERT INTO %s \n" % safe_table_name(self.model_class)
        _sql += "("

        # add attribute names, taking first model
        for _, attr in put_items[0].get_attributes().items():
            _sql += f'"{attr.attr_name}", '

        _sql = _sql[:-2]
        _sql += ")\nVALUES \n"

        # if we have duplicates by primary key, take only the last value
        # model_class = put_items[0].__class__
        if self.model_class._range_key_attribute() and self.model_class._hash_key_attribute():
            unique_on = [self.model_class._hash_key_attribute(), self.model_class._range_key_attribute()]
        else:
            unique_on = [self.model_class._hash_key_attribute()]

        unique_put_items = {}
        for model_instance in put_items:
            # simple_serialized = model_instance.to_simple_dict(force=True)
            # dynamo_serialized = model_instance.to_dynamodb_dict()
            # # model_args = model_instance.get_save_kwargs_from_instance()['Item']
            uniq_key = ":".join([f'{self._attribute_value(model_instance, attr)}' for attr in unique_on])
            # uniq_key = ":".join([f'{getattr(model_instance, attr.attr_name) for attr in unique_on}'])
            log.debug(f'UNIQ_KEY: {uniq_key}')
            unique_put_items[uniq_key] = model_instance

        for item in unique_put_items.values():
            _sql += "\t(" + self._attribute_values(item) + "),\n"

        _sql = _sql[:-2] + ";"

        log.debug('SQL: %s' % _sql)

        return _sql

    def insert_into(self, conn: sqlite3.Connection, put_items: List[_T]):
        """perform the INSERT INTO SQL operation"""

        statement = self.insert_statement(put_items)

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
        except Exception as e:
            log.error(e)
            raise


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
