# Shared Indices

While Hamilton is a general-purpose framework, we've found a common pattern is to manipulate datasets that have shared indices (spines). Although this might not apply towards every use-case (E.G. more complex joins with spark dataframes), a large selection of use-cases can be enabled if every dataframe in your pipeline shares an index. This is particularly pertinent when writing transformations over (non-event-based) time-series data.

While hamilton currently has no means of enforcing shared-spine, it is up to the writer of the function to validate input data as necessary.&#x20;
