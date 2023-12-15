# Purpose of this module
This module provides a simple dataflow to generate an image using openAI DallE generation capabilities.

# Configuration Options
This module can be configured with the following options:
[list options]

- `base_prompt` -- prompt to generate from a string
- `style` -- string for the style of the image to generate, defaults to "realist"
- `size` -- string for the size of the image to generate, defaults to "1024x1024"
- `hd` -- Whether to use high definition, defaults to False

# Limitations

Pretty simple -- only exposes the above parameters.
