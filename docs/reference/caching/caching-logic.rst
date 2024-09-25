=======================
Caching logic
=======================

Caching Behavior
----------------

.. autoclass:: hamilton.caching.adapter.CachingBehavior
   :exclude-members: __init__, from_string
   :members:

   .. automethod:: from_string


``@cache`` decorator
----------------------

.. autoclass:: hamilton.function_modifiers.metadata.cache
   :special-members: __init__
   :private-members:
   :members:
   :exclude-members: BEHAVIOR_KEY, FORMAT_KEY

   .. autoattribute:: BEHAVIOR_KEY

   .. autoattribute:: FORMAT_KEY


Logging
-----------

.. autoclass:: hamilton.caching.adapter.CachingEvent
   :special-members: __init__
   :members:
   :inherited-members:


.. autoclass:: hamilton.caching.adapter.CachingEventType
   :special-members: __init__
   :members:
   :inherited-members:


Adapter
--------

.. autoclass:: hamilton.caching.adapter.SmartCacheAdapter
   :special-members: __init__
   :members:
   :inherited-members:
