from collections.abc import Callable

from beartype import BeartypeConf, BeartypeStrategy, beartype
from typing_extensions import ParamSpec, TypeVar

no_bear_type_check_conf = BeartypeConf(strategy=BeartypeStrategy.O0)

no_bear_type = beartype(conf=no_bear_type_check_conf)

P = ParamSpec("P")
R = TypeVar("R")


def no_bear_type_check(func: Callable[P, R]) -> Callable[P, R]:
    return no_bear_type(func)


bear_spray = no_bear_type_check
