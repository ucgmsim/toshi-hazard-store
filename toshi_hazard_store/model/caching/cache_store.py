import logging
import math
import pathlib
import sqlite3
from typing import Iterable

from pynamodb.expressions.condition import Condition

from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER

# from toshi_hazard_store.db_adapter.sqlite.sqlite_store import (  # noqa
#     ensure_table_exists,
#     execute_sql,
#     get_model,
#     put_model,
#     safe_table_name,

# )

# from toshi_hazard_store.db_adapter.sqlite.pynamodb_sql import (
#     safe_table_name,
#     get_version_attribute,
#     SqlWriteAdapter,
#     SqlReadAdapter,
# )


log = logging.getLogger(__name__)


def get_connection(model_class) -> sqlite3.Connection:
    if not cache_enabled():
        raise RuntimeError("cannot create connection ")
    log.info(f"get cache connection for {model_class} using path {LOCAL_CACHE_FOLDER}/{DEPLOYMENT_STAGE}")
    return sqlite3.connect(pathlib.Path(str(LOCAL_CACHE_FOLDER), DEPLOYMENT_STAGE))


def cache_enabled() -> bool:
    """return True if the cache is correctly configured."""
    if LOCAL_CACHE_FOLDER is not None:
        if pathlib.Path(LOCAL_CACHE_FOLDER).exists():
            return True
        else:
            log.warning(f"Configured cache folder {LOCAL_CACHE_FOLDER} does not exist. Caching is disabled")
            return False
    else:
        log.warning("Local caching is disabled, please check config settings")
        return False


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
