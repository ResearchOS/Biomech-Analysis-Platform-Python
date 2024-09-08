A type of [Runnable](../Runnables/index.md)

Preferred format is:
```toml
[logsheet]
num_header_rows = 1
dataset_factors = ["{factor1}", "{factor2}"] # In hierarchical order. The first factor is the highest level of the hierarchy.
path = "path/to/logsheet.csv"

[logsheet.headers]
{factor1}.column_name = "{Column Header Name}"
{factor1}.type = "{type}"
{factor1}.level = "{level}"
```

# Attributes
## Headers
A dictionary where each key is a `{Column Header Name}` (the name of the column in the logsheet) and `{type}` is the type of the column. The type can be one of the following: `"str"`, `"num"`, `"bool"`. 

## Outputs
A list of strings where each string is the name of an output. This is generated from the headers' names.

## Num Header Rows
An integer describing the number of rows that are headers in the logsheet. For example, if set to 3, then the 4th row is the first row of data.

## Dataset Factors
An ordered list of the variables that are factors in the dataset. The first factor is the highest level of the hierarchy.