# Relational Database Query Optimization

This is a simple RDBMS optimiser. It accepts a accept a canonical query plan (a project over a series of selects over a cartesian product over the input named relations) and aim to construct a left-deep query plan which minimises the sizes of any intermediate relations.
