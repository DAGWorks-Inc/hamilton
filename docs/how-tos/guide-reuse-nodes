# Guide for Using Node Reuse in Hamilton 📖

**What is Node Reuse? 🔍**

◉ Hamilton is this Python library for building Directed Acyclic Graphs (DAGs) for data transformations, right?

In a DAG, you write functions (called nodes) that do stuff with your data. Sometimes, you might need the same data in different places. Instead of calculating it over and over again, you can reuse nodes.

◉ For example, say you calculate __daily_sales.__ 
You might need to use that to calculate both __weekly_sales__ and __monthly_sales__. 

Instead of re calculating __daily_sales__ every time, you can just reuse it! Makes life easier and your code faster.

# Why Node Reuse is Important ⚡:

**There are a few reasons why node reuse is super useful:**

1. Faster execution: Hamilton calculates the node once and reuses the result, so you don’t waste time running the same calculations multiple times.

2. Less code duplication: You write the logic once and reuse it wherever it’s needed this makes your code easier to maintain.
 
3. Consistency: By reusing a node, you’re making sure that everywhere it’s used gets the exact same result, avoiding mistakes or mismatched data.


# Example of Node Reuse in Hamilton 💻:


A Simple yet easy way to show how node reuse works in hamilton!:

    import hamilton
    
    # Function that calculates daily sales
    def daily_sales(raw_data):
        return raw_data.sum(axis=0)
    
    # Reuse daily_sales to calculate weekly and monthly sales
    def weekly_sales(daily_sales):
        return daily_sales * 7
    
    def monthly_sales(daily_sales):
        return daily_sales * 30


# In the above example:

◉ The function __daily_sales__ is calculated once.

◉ Both __weekly_sales__ and __monthly_sales__ reuse __daily_sales__, making things faster and more efficient!


# Want to Learn More? Check These Out 📚

If you are curious and want to dive deeper into node reuse, here are some resources that explain it well:

◉ Hamilton Meetup Slides (March 2024) -> https://github.com/skrawcz/talks/files/14657471/Hamilton.March.2024.Meetup.pdf

The slides go into detail about node reuse (check out slides 10-12). It’s a short one with some helpful examples!

◉ YouTube Deep Dive -> https://www.youtube.com/watch?v=IJByeN41xHs

Check out this YouTube video from the Hamilton meetup. If you skip to around 20:35, you’ll find the part that talks specifically about node reuse.

◉ Notebook -> https://github.com/DAGWorks-Inc/hamilton-tutorials/blob/main/2024-03-19/march-meetup.ipynb

It has a lot of examples.

# Example Code in the Hamilton Repo 🛠️:

**You can also see node reuse with the examples in the Hamilton GitHub repository. Here are a few of them:**

◉ examples/node_reuse_example_1.py: A basic example showing node reuse.

◉ examples/node_reuse_example_2.py: A more advanced example with multiple data streams.


**To try them out, clone the repository and run the Python files like this:**

    git clone https://github.com/dagworks-inc/hamilton.git
    cd hamilton/examples
    python node_reuse_example_1.py


# Tips for Using Node Reuse 💡:

**These will Help you make the most of node reuse in Hamilton:**

1. Plan ahead: Define reusable nodes early in your DAG so they’re ready for use wherever you need them.

2. Use descriptive names: Clear names for your nodes help you (and others) understand what each node does and where it can be reused.

3. Reuse wherever you can: Don’t duplicate calculations, if the data already exists, reuse it!


# Conclusion 🔚

Node reuse is a simple but powerful feature in Hamilton that makes your DAGs faster, cleaner, and easier to manage.

Instead of recalculating the same thing over and over, you can reuse nodes to keep things efficient.


# If you want to learn more, definitely check out the slides, videos, and examples mentioned above.

# Thank You
