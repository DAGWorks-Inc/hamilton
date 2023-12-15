# Purpose of this module
This module allows you to convert a variety of images in S3, placing them next to the originals.

It handles anything that [pillow](https://pillow.readthedocs.io/en/stable/) can handle.

# Configuration Options
This modules takes in no configuration options. It does take in the following parameters as inputs:
- `path_filter` -- a lambda function to take in a path and return `True` if you want to convert it and `False` otherwise. This defaults to checking if the path extension is `.png`.
- `prefix` -- a prefix inside the bucket to look for images.
- `bucket` -- the bucket to look for images in.
- `new_format` -- the format to convert the images to.
- `image_params` -- a dictionary of parameters to pass to the `Image.save` function. This defaults to `None`, which gets read as an empty dictionary.

# Limitations
Assumes:
1. The files are labeled by extension correctly
2. They are all in the same format
