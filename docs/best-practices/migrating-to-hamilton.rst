=====================
Migrating to Hamilton
=====================

Continuous Integration for Comparisons
--------------------------------------

Create a way to easily & frequently compare results.

#. Integrate with continuous integration (CI) system if you can.
#. 🔎🐛  Having a means that tests code early & often will helps diagnose bugs in your old code (most likely) or your new implementation (less likely).
#. Specifically, have a system to compare the output of your Hamilton code, to compare to the output of your existing system.

.. image:: ../_static/Hamilton_ApplyMeetup_2022_migration_CI_1.svg
  :alt: Example CI process that we used at Stitch Fix for migrating to Hamilton

Integrate into your code base via a `"custom wrapper object"`
-------------------------------------------------------------

If you have existing systems that you want to integrate Hamilton into, it might require non-trivial effort for you to
change those systems to be able to use Hamilton. If that's the case, then we suggest creating a "custom object" to
"wrap" Hamilton, so that it's easier to migrate to it.

Specifically, this custom wrapper object class's purpose is to match your existing API expectations. It will act as the
translation layer from your existing API expectations, to what running Hamilton requires, and back. In Hamilton
terminology, this is a `Custom Driver Wrapper`, since it wraps around the Hamilton Driver class.

.. image:: ../_static/Hamilton_ApplyMeetup_2022_wrapper.svg
  :alt: The wrapper driver class helps ensure your existing API expectations are matched.

This is a best practice because:

#. When migrating, it's best to avoid making too many changes. So don't change your API expectations if you can.
#. It allows you to easily insert Hamilton into any context. Thereby minimizing potential migration problems.
