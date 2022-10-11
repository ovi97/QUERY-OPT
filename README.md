# Relational Database Query Optimization

This is a simple RDBMS optimiser. It accepts a accept a canonical query plan (a project over a series of selects over a cartesian product over the input named relations) and aim to construct a left-deep query plan which minimises the sizes of any intermediate relations. The following are a decription of its files .

SJDB supports a limited subset of the relational algebra, consisting of the following operators only:

[^1]cartesian product
[^2] select with a predicate of the form attr=val or attr=attr
[^3] project
[^4] equijoin with a predicate of the form attr=attr
[^5]scan (an operator that reads a named relation as a source for a query plan)

## Estimator.java
Estimator implements the PlanVisitor interface and performs a depth-first traversal of the query plan

## Optimiser.java
Optimiser takes a canonical query plan as input, and produce an optimised query plan as output.


