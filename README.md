# Building solid data pipelines with PySpark

  
📚 A course brought to you by the [Data Minded Academy].

## Context

These are the exercises used in the course *Building solid data pipelines with
PySpark*, reduced to the set that can be handled during our summer/winter
school format (that's a heavy reduction: from 12h of interactive moments to
about 5h).  The course has been developed by instructors at Data Minded. The
exercises are meant to be completed in the lexicographical order determined by
name of their parent folders. That is, exercises inside the folder `b_foo`
should be completed before those in `c_bar`, but both should come after those
of `a_foo_bar`.

## Getting started

While you can clone the repo locally, we do not offer support for setting up
your coding environment. Instead, we recommend you [tackle the exercises
using Gitpod][this gitpod].

[![Open in Gitpod][gitpod logo]][this gitpod]


⚠ IMPORTANT: After about 30 minutes of inactivity the Gitpod environment shuts down and
you will lose unsaved progress.  If you want to save your work, either fork the
repo or create a branch (permissions needed). Don't forget to regularly sync
the upstream repo's changes in case of a fork, so that you can get the
solutions.

# Course objectives

- Introduce good data engineering practices.
- Illustrate modular and easily testable data transformation pipelines using
  PySpark.
- Illustrate PySpark concepts, like lazy evaluation, caching & partitioning.
  Not limited to these three though.

# Intended audience

- People working with (Py)Spark or soon to be working with it.
- Familiar with Python functions, variables and the container data types of
  `list`, `tuple`, `dict`, and `set`.

# Approach

Lecturer first sets the foundations right for Python development and
gradually builds up to PySpark data pipelines.

There is a high degree of participation expected from the students: they
will need to write code themselves and reason on topics, so that they can
better retain the knowledge. 
  
Participants are recommended to be working on a branch for any changes they
make, to avoid conflicts (otherwise the onus is on the participant), as the
instructors may choose to release an update to the current branch.

Note: this course is not about writing the best pipelines possible. There are
many ways to skin a cat, in this course we show one (or sometimes a few), which
should be suitable for the level of the participants.

# Exercises

## Warm-up: being comfortable with pytest

#### Rationale

This exercise is a warm-up and has nothing to do with PySpark. It is the
_first_ exercise though, because with most subsequent exercises you can verify
your own solutions simply by running the particular test written for whatever
it is that you need to implement. This allows for greater autonomy.
Additionally, this is exactly what you should be doing when you write code in a
professional setting: have (and run) tests, so that you can make improvements
that don't break the desired behaviour, or—when it does break—you're at least
informed and can take action.

Finally, it will allow yourself and the instructor to get a feeling for how
much of Python you know so that you and the instructor can be proactive about
asking for/providing assistance.

#### Requirement

Look into the
[exercises/b_unit_test_demo/distance_metrics.py](exercises/b_unit_test_demo/distance_metrics.py)
module. It contains a function that implements some mathematical formula known
as the Haversine or great-circle-distance. Or at least it was an attempt. 

Your task is to write at least one test that shows that the function as it is
implemented is wrong. 

When you have proven that, the instructor will show what was wrong and how you
can improve your little test suite.

#### Objective

You will know how to

- run a Python test, using the `pytest` command on the command line interface.
- design tests that make useful abstractions for colleagues
- understand dependency inversal

### Adding derived columns

Check out [dates.py](exercises/c_labellers/dates.py) and implement the pure
Python function `is_belgian_holiday`. verify your correct implementation by
running the test `test_pure_python_function` from
[test_labellers](tests/test_labellers.py). you could do this from the command
line with `pytest tests/test_labellers.py::test_pure_python_function`. Note that this has nothing to do with PySpark yet, but it might just come in handy for a subsequent exercise…

Return to [dates.py](exercises/c_labellers/dates.py) and implement
`label_weekend`. This is your first encounter with PySpark in these exercises.
Again, run the related test from [test_labellers.py](tests/test_labellers.py).
It might be more useful to you if you first read the test and try to understand
what it is testing for. Tests often enforce the functional requirements.

Finally, implement `label_holidays` from [dates](exercises/c_labellers/dates.py). 
As before, run the relevant test to verify a few easy cases (keep in mind that 
few tests are exhaustive: it's typically easier to prove something is wrong, 
than that something is right).

If you're making great speed, try to think of an alternative implementation 
to `label_holidays` and discuss pros and cons.

### Common business case: cleaning data

Using the information seen in the videos, prepare a sizeable dataset for 
storage in "the clean zone" of a data lake, by implementing the `clean` 
function of [clean_flights_starter.py](exercises/h_cleansers/clean_flights_starter.py).

As a small aside: "clean zone" is arguably a better name for this part of your
data storage layer than "silver" (raw-clean-business vs bronze-silver-gold),
but in practice you'll hear all these variants. Try to keep them straight in
your head.

### Peer review (optional)

In group, discuss the improvements one could make to 
[bingewatching.py](./exercises/d_code_review/bingewatching.py).

## Solutions

Solutions to the exercises are in the solutions.tb64 file, which is a base64
encoded tarball. The tarball was zipped using gzip. That should provide most
people with some bash experience the information they need to extract this.

Putting solutions on a different branch has some downsides, such as making it
hard to keep partial files (the starters) aligned with their solutions in, for
example, the function definitions.

[this gitpod]: https://gitpod.io/#https://github.com/datamindedacademy/effective_pyspark
[gitpod logo]: https://gitpod.io/button/open-in-gitpod.svg
[Data Minded Academy]: https://www.dataminded.academy/
