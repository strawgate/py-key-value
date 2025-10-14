from collections.abc import Callable
from typing import Any

from beartype import BeartypeConf, BeartypeStrategy, beartype

no_bear_type_check_conf = BeartypeConf(strategy=BeartypeStrategy.O0)

no_bear_type = beartype(conf=no_bear_type_check_conf)


def no_bear_type_check(func: Callable[..., Any]) -> Callable[..., Any]:
    return no_bear_type(func)


bear_spray = no_bear_type_check
