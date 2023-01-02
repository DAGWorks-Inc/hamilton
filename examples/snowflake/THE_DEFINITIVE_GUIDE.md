# Running Hamilton on Snowflake's Snowpark
Here's a rather long explanation of how to run Hamilton on top of Snowpark.
Grab your favorite beverage and settle in.

# What is Snowpark?
Snowflake's Snowpark product is a platform for building, storing, and executing data pipelines and
transformations. It is designed to be fully integrated with Snowflake's cloud-based data warehouse,
allowing users to easily create data pipelines that extract data from various sources, transform it
as needed, and load it into Snowflake for analysis and reporting. Snowpark is a key component of
Snowflake's data integration offering.

# Understanding your options
If you're wondering "how do I get started?", this next section is for you. I'm not going to lie,
the Snowflake documentation doesn't make it that straight forward to understand what your options are,
and when to use what. I'm going to try to make it a little easier for you.

There are two ways to run python code with Snowpark. The first is to use the Snowpark Python API.
The second is to use their SQL syntax and define your python code that way. Ultimately they do the same thing,
but are different APIs. The former is mainly used from python/jupyter notebooks, and the latter from SQL clients/Snowflake's web UI.

In terms of what python code you can run? There are three options:

* Stored Procedures
* User Defined Functions, aka UDFs, along with their cousin Batch UDFs
* User Defined Table functions, aka UDTFs

## Stored Procedures
The idea behind stored procedures is that you don't call them within a SQL query that you're building. Instead
they are either triggered with a direct invocation `CALL my_procedure(...)`, or as part of a trigger on a table. That is,
they are basically some "compute" jobs using Snowflake as the compute framework.
They are also powerful and you can do anything you want to your warehouse basically: they can read from tables, write to tables,
create ML models, pickle ML models to a stage, etc.


## User Defined Functions (UDFs) and Batch UDFs
With Python user-defined functions (UDFs), you can write Python code and call it as though it were a SQL function. The
code you write assumes that you're operating on a single row of data at a time and outputting a single row of data. In the
batch UDF case, the assumption is that you're operating over a Pandas dataframe of data, and returning a Pandas series/array
of data.

Quirks: Function arguments are bound by position, not name. The first argument passed to the UDF is the first argument received by the Python function.


## User Defined Functions (UDFs)


## User Defined Table Functions (UDTFs)




But first, let's digress and talk about python dependencies.

## Python dependencies
Firstly, Snowflake only supports Python 3.8. So if you're wanting something newer, or
older, you're out of luck for the time being.

Secondly, it's not straightforward to "install" python dependencies to run on Snowpark. You have
two options:

1. Enable a curated Anaconda package.
2. Manually upload python code and packages to Snowflake.

To run Hamilton, you'll need to do both.

### Enabling curated Anaconda packages
Snowflake has a [curated list of python packages](https://repo.anaconda.com/pkgs/snowflake/) that you can enable
for your Snowflake data warehouse. For reference here's [the link to documentation](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html)
on how you enable this. In short, you need to log into "Snowsight" the Snowflake UI and accept
their terms and conditions to get access to the curated packages. The steps should look something like this:

1. Log into [Snowsight](https://app.snowflake.com), the Snowflake web interface.
2. Click the dropdown menu next to your login name, then click Switch Role » ORGADMIN to change to the organization administrator role.
3. Click Admin » Billing » Terms & Billing.
4. Scroll to the Anaconda section and click the Enable button. The Anaconda Packages (Preview Feature) dialog opens.
5. Click the link to review the Snowflake Third Party Terms.
6. If you agree to the terms, click the Acknowledge & Continue button.

To validate the above worked, you should then be able to run the following query:

```sql
select * from information_schema.packages where language = 'python';
```
Which will display the list of python packages available and what their versions are.

### Getting python code and packages to Snowflake
If you're writing some simple python transformations, you don't need to upload anything. You'll just "inline" define
this python code in the SQL, or use the Snowpark Python API; see next sections for more details. For example, you have a handful of
functions that only depend on python libraries found in the curated list. E.g. fitting a scikit-learn model, or shifting
the values of a column for time-series modeling with Pandas.

If instead you depend on a library that is not in the curate list, e.g. like Hamilton, or you have some homegrown library or package, you'll
need to get all that python code and its dependencies into Snowflake. To do this you'll upload the code to a "stage".
A "stage" is a location in Snowflake where you can upload files. Think of it as a directory you upload code to.
You can then reference this "stage" in your SQL or Python UDF definitions.

Once the code is uploaded to a stage, you can then reference it when you make your python UDF definitions.

So at a high level you need to:

1. Cross-reference your import statements with what's available in the curated list. If that library is in the
curated list, you're good to go.
2. If not, you'll need to upload your code and any python dependencies that aren't there to Snowflake. Which will be
the case when you're using Hamilton.
3. Define your python UDFs in Snowflake. This is where you'll reference the code you uploaded to a stage location, and/or the
curated list of python packages.



## Snowpark Python API
https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html

## Defining Python Transforms via Snowpark SQL
https://docs.snowflake.com/en/developer-guide/udf/python/udf-python.html

### Uploading python code and packages to Snowflake
So how do you get a library like Hamilton into Snowflake? Well, you'll need to do the following:
1. Create a "zip" file of Hamilton. E.g. either checkout the repository and zip the "hamilton" directory within the repository, or
`pip install sf-hamilton` and then under the site-packages folder of your python environment, zip the "hamilton" directory. E.g.
`zip -r hamilton.zip ~/.pyenv/versions/3.9.13/lib/python3.9/site-packages/hamilton` if you're using pyenv and python 3.9.13 installed.
2. Upload the zip file to a "stage" in Snowflake via the SnowSQL CLI:
```bash
# upload Hamilton package to Snowflake
snowsql -q "put
file://hamilton.zip
@~/
auto_compress = false
overwrite = true
;"
# upload your hamilton functions
snowsql -q "put
file://my_functions.py
@~/
auto_compress = false
overwrite = true
;"
# upload hamilton driver
snowsql -q "put
file://my_functions.py
@~/
auto_compress = false
overwrite = true
;"
```
