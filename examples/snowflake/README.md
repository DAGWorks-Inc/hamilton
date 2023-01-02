# Here's how to get Hamilton running on Snowflake's Snowpark
Let's start with a high level overview of the steps you need to take.

1. Develop your Hamilton code outside of Snowpark. Why? You'll develop much faster. E.g. pull a minimal dataset down
for local/notebook development and develop that way.
2. Enable the third party curated Anaconda packages in Snowflake. This is a one-time setup step. See below for instructions.
3. Upload the Hamilton package to Snowflake.
4. Upload your Hamilton code to a Snowflake stage.
5. Define either a stored procedure, a user defined function (UDF), or a user defined table function (UDTF) in Snowflake.
What you choose here depends on the output structure of what you're doing with Hamilton. More details below.
6. Call the code you defined in (5) in the appropriate way.

On first glance, this doesn't look like it should be too bad right? Well, it's not too bad. But it's not trivial either,
and definitely a bit of a learning curve the first time through. Let's walk through each step in detail.

# 0. Prerequisite
You need to have installed the Snowpark Python API & and Hamilton. The reference documentation can be found here - [Snowpark Python API](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html).

Using your favorite means to manage a python environment, install the Snowpark Python API.
E.g.
```bash
# activate your virtual environment, and then:
pip install snowflake-snowpark-python
pip install sf-hamilton
# and install any other packages you need
```

What is the Snowpark Python API? It's a python library that allows you to write python code that interacts
with Snowflake. You can use it to pull dataframes, as well as to define UDFs, UDTFs, and stored procedures.


# 1. Develop your Hamilton code outside of Snowpark
We won't go into detail here, as we assume familiarity with Hamilton. For reference here's a few blog posts in case
you need help:
* Thierry's post
* Tidy Production Pandas
* Iterating with Hamilton in a notebook

# 2. Enabling curated Anaconda packages
To enable Hamilton to run, you need to enable the [curated list of python packages](https://repo.anaconda.com/pkgs/snowflake/) in Snowflake.
This is a one-time setup step. For reference here's [the link to documentation](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html)
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

# 3. Uploading Hamilton to Snowflake
So that your code can run, it needs to have the Hamilton library code available. To do this, you need to upload the
source distribution of the Hamilton code in zip format.
Here are the steps for this:

1. Download the latest Hamilton source distribution from [PyPi](https://pypi.org/project/sf-hamilton/#files). The one with the `tar.gz` ending.
2. Decompress the file. E.g. `tar -xzf sf-hamilton-1.12.0.tar.gz`. This will create a directory called `sf-hamilton-1.12.0`.
3. Zip up the sf-hamilton-X.X.X directory. E.g. `zip -r sf-hamilton-1.12.0.zip sf-hamilton-1.12.0`. This will create a zip file called `sf-hamilton-1.12.0.zip`.
4. Upload the zip file to a Snowflake stage. E.g. `put file://sf-hamilton-1.12.0.zip @~/sf-hamilton-1.12.0.zip`.

Note for each release of Hamilton, you'll need to perform the steps above if you want to use the latest version of Hamilton.

# 4. Uploading your Hamilton code to a Snowflake stage
Now that you have Hamilton available, you need to upload your Hamilton code to a Snowflake stage. That is, the modules
where your Hamilton functions are defined, and then any driver code that you have that tells Hamilton what to do.

Here are the steps for this:

1. If your code doesn't consist of many files, then probably the easiest thing to do is individually upload each file to a Snowflake stage.
E.g.
2. If your code consists of many files, then you can zip up the files and upload the zip file to a Snowflake stage.

# 5. Defining a stored procedure, UDF, or UDTF in Snowflake
Now that you have Hamilton available, and you have your Hamilton code available, you need to defined something to call it.
But what you define here depends on the output of what you're getting Hamilton to do. Use a:

* User Defined Function (UDF) if the result of using Hamilton only returns a single "column" of data, e.g. a prediction.
* User Defined Table Function (UDTF) if the result of using Hamilton returns multiple "columns" of data, e.g. training data.
* Stored Procedure if you want to do anything else.
