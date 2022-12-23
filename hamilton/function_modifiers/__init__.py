import logging

from . import base, configuration, dependencies, expanders, macros, metadata, validation

logger = logging.getLogger(__name__)

"""
Annotations for modifying the way functions get added to the DAG.
All user-facing annotation classes are lowercase as they're meant to be used
as annotations. They are classes to hold state and subclass common functionality.
"""

# These all represent the public API for function_modifiers
# All new user-facing decorators/helper functions should be here

# Backwards-compatibility to be safe
InvalidDecoratorException = base.InvalidDecoratorException

# The config decorator
config = configuration.config

# Dependency Specification
# Helper functions to specify dependency sources for parameterization
value = dependencies.value
source = dependencies.source

# These aren't strictly part of the API but we should have them here for safety
LiteralDependency = dependencies.LiteralDependency
UpstreamDependency = dependencies.UpstreamDependency

# Parameterization decorators (both the old and new ones)
# The three "blessed" @parameterize decorators
parameterize = expanders.parameterize
parameterize_sources = expanders.parameterize_sources
parameterize_values = expanders.parameterize_values

# The older ones that will be deprecated
parametrized = expanders.parametrized
parameterized_inputs = expanders.parameterized_inputs
parametrized_input = expanders.parametrized_input

# Extract decorators
extract_columns = expanders.extract_columns
extract_fields = expanders.extract_fields

# does decorator
does = macros.does

# dynamic transform/model decorator
dynamic_transform = macros.dynamic_transform
model = macros.model

# Metadata-specifying decorators
tag = metadata.tag
tag_outputs = metadata.tag_outputs

# data quality + associated tags
check_output = validation.check_output
check_output_custom = validation.check_output_custom
IS_DATA_VALIDATOR_TAG = validation.IS_DATA_VALIDATOR_TAG
DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG = validation.DATA_VALIDATOR_ORIGINAL_OUTPUT_TAG
