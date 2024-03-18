"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.

The idea behind this exercise is to get you familiar with testing, so that when
you develop code, you can quickly iterate and check that what you're developing
matches the expectations.
"""

from exercises.b_unit_test_demo.distance_metrics import great_circle_distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    pass


PI = 3.14159265359
def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    pass

def test_great_circle_distance1():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    latitude1 = -90
    longitude1 = 0
    latitude2 = 90
    longitude2 = 0
    sol = great_circle_distance(latitude1, longitude1, latitude2, longitude2, radius = 1)
    print (sol, 1 * PI) 
    assert round(sol, 4) == round(1 * PI, 4)


def test_great_circle_distance2():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    latitude1 = 0
    longitude1 = 0
    latitude2 = 0
    longitude2 = 180
    sol = great_circle_distance(latitude1, longitude1, latitude2, longitude2, radius = 1)
    print (sol, 1 * PI) 
    assert round(sol, 4) == round(1 * PI, 4)   


def test_great_circle_distance3():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    latitude1 = 0
    longitude1 = 0
    latitude2 = 0
    longitude2 = 90
    sol = great_circle_distance(latitude1, longitude1, latitude2, longitude2, radius = 1)
    print (sol, 1 * PI/2) 
    assert round(sol, 4) == round(1 * PI/2, 4)     


def test_great_circle_distance4():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?
    latitude1 = 0
    longitude1 = 0
    latitude2 = 90
    longitude2 = 0
    sol = great_circle_distance(latitude1, longitude1, latitude2, longitude2, radius = 1)
    print (sol, 1 * PI/2) 
    assert round(sol, 4) == round(1 * PI/2, 4)        