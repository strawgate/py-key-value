import os

from beartype._conf.confenum import BeartypeStrategy
from beartype._conf.confmain import BeartypeConf  # type: ignore
from beartype.claw import beartype_this_package  # type: ignore

disable_beartype = os.environ.get("PY_KEY_VALUE_DISABLE_BEARTYPE", "false").lower() in ("true", "1", "yes")

strategy = BeartypeStrategy.O0 if disable_beartype else BeartypeStrategy.O1

beartype_this_package(conf=BeartypeConf(violation_type=UserWarning, strategy=strategy))
