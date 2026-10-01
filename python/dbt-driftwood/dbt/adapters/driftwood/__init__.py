import os

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.driftwood.__version__ import version as version  # noqa: PLC0414 — re-exported as public API

__version__ = version
from dbt.adapters.driftwood.connections import DriftwoodConnectionManager as DriftwoodConnectionManager
from dbt.adapters.driftwood.connections import DriftwoodConnectionManager as FelderaConnectionManager
from dbt.adapters.driftwood.credentials import DriftwoodCredentials, DriftwoodCredentials as FelderaCredentials
from dbt.adapters.driftwood.impl import DriftwoodAdapter, DriftwoodAdapter as FelderaAdapter

Plugin = AdapterPlugin(
    adapter=DriftwoodAdapter,
    credentials=DriftwoodCredentials,
    include_path=os.path.join(os.path.dirname(__file__), "..", "..", "include", "driftwood"),
)
