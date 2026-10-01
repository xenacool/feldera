from driftwood.rest.feldera_client import (
    FelderaClient as DriftwoodClient,
    FelderaClient as FelderaClient,
)
from driftwood.rest.retry import RetryConfig as RetryConfig
from driftwood.pipeline import Pipeline as Pipeline
from driftwood.pipeline_builder import PipelineBuilder as PipelineBuilder
from driftwood.rest._helpers import determine_client_version

__version__ = determine_client_version()

import pretty_errors

pretty_errors.configure(
    line_number_first=True,
)

pretty_errors.activate()
