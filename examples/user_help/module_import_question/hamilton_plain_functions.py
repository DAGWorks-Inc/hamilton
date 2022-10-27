"""Module that brings in Hamilton as a dependency"""

import plain_functions

from hamilton import function_modifiers

moving_average__v1 = function_modifiers.config.when(ma_type="v1")(
    plain_functions.moving_average__v1
)

moving_average__v2 = function_modifiers.config.when(ma_type="v2")(
    plain_functions.moving_average__v2
)

some_other_function = plain_functions.some_other_function
