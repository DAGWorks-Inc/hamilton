=======================
Caching logic
=======================

Caching Behavior
----------------

.. autoclass:: hamilton.lifecycle.caching.CachingBehavior
   :exclude-members: __init__, from_string
   :members:

   .. automethod:: from_string


``@cache`` decorator
----------------------

.. autofunction:: hamilton.function_modifiers.metadata.cache

.. autoclass:: hamilton.function_modifiers.metadata.Cache
   :special-members: __init__
   :private-members:
   :members:
   :exclude-members: BEHAVIOR_KEY, FORMAT_KEY

   .. autoattribute:: BEHAVIOR_KEY

   .. autoattribute:: FORMAT_KEY


Logging
-----------

.. autoclass:: hamilton.lifecycle.caching.CachingEvent
   :special-members: __init__
   :members:
   :inherited-members:


.. autoclass:: hamilton.lifecycle.caching.CachingEventType
   :special-members: __init__
   :members:
   :inherited-members:


Adapter
--------

.. autoclass:: hamilton.lifecycle.caching.SmartCacheAdapter
   :special-members: __init__
   :members:
   :inherited-members:
