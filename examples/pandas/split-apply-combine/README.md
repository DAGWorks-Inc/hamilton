# Split / Apply / Combine

This example demonstrates how to perform a split-apply-combine transformation using Hamilton.

Many data analysis or processing involve one or more of the following steps:

 - **Split**: splitting a data set into groups,
 - **Apply**: applying some functions to each of the groups,
 - **Combine**: combining the results.

For this example, we want to **split** a DataFrame using a partition key (static partitions), then for each partition **apply** 
a different transformation pipeline then combine the result in a DataFrame.

