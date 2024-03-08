# Split / Apply / Combine

This example demonstrates how to perform
a [split-apply-combine](https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html) transformation using
Hamilton.

Many data analysis or processing involve one or more of the following steps:

- **Split**: splitting a data set into groups,
- **Apply**: applying some functions to each of the groups,
- **Combine**: combining the results.

For this example, we want to **split** a DataFrame using a partition key (static partitions), then for each partition *
*apply**
a different transformation pipeline then combine the result in a DataFrame.

## Example

The example consists of calculate the tax of individuals or families based on their income and the number of children
they have.

The following rules applies to Income:

- < 50k: Tax rate is 15 %
- 50k to 70k: Tax rate is 18 %
- 70k to 100k: Tax rate is 20 %
- 100k to 120k: Tax rate is 22 %
- 120k to 150k: Tax rate is 25 %
- over 150k: Tax rate is 28 %

The following rules applies to the number of children when the income is over 100k:

- 0 child: Tax credit 0 %
- 1 child: Tax credit 2 %
- 2 children: Tax credit 4 %
- 3 children: Tax credit 6 %
- 4 children: Tax credit 8 %
- over 4 children: Tax credit 10 %

The following data needs to be processed:

| Name     | Income | Children | 
|----------|--------|----------|
| John     | 75600  | 2        |
| Bob      | 34000  | 0        |
| Chloe    | 111500 | 3        |
| Thomas   | 234546 | 1        |
| Ellis    | 144865 | 2        |
| Deane    | 138500 | 4        |
| Mariella | 69412  | 5        |
| Carlos   | 65535  | 0        |
| Toney    | 43642  | 3        |
| Ramiro   | 117850 | 2        |  

The expected result is :

| Name     | Income | Children | Tax Rate | Tax credit | Tax   | Formula                                  |   
|----------|--------|----------|----------|------------|-------|------------------------------------------|    
| John     | 75600  | 2        | 18       |            | 13608 | (75600 * 0.18)                           |    
| Bob      | 34000  | 0        | 15       |            | 5100  | (34000 * 0.1)                            |    
| Chloe    | 111500 | 3        | 22       | 6          | 23058 | (111500 * 0.22) - (111500 * 0.22) * 0.06 |    
| Thomas   | 234546 | 1        | 28       | 2          | 64359 | (234546 * 0.28) - (234546 * 0.28) * 0.02 |    
| Ellis    | 144865 | 2        | 25       | 4          | 34768 | (144865 * 0.25) - (144865 * 0.25) * 0.04 |    
| Deane    | 138500 | 4        | 25       | 8          | 31855 | (138500 * 0.25) - (138500 * 0.25) * 0.08 |    
| Mariella | 69412  | 5        | 20       |            | 13882 | (69412 * 0.2)                            |    
| Carlos   | 65535  | 0        | 18       |            | 11796 | (65535 * 0.18)                           |    
| Toney    | 43642  | 3        | 15       | 6          | 6154  | (43642 * 0.15) - (43642 * 0.15) * 0.06   |    
| Ramiro   | 117850 | 2        | 22       | 4          | 24890 | (117850 * 0.22) - (117850 * 0.22) * 0.04 |    

For the sake of this example the Dataframe is partition in 2 dataframes:

- **Income under 100k**: A pipeline process that Dataframe to apply Tax Rate then apply Tax Credit
- **Income over 100k** : A pipeline process that Dataframe to apply only Tax Rate