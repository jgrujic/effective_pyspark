"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.

The idea behind this exercise is to get you familiar with testing, so that when
you develop code, you can quickly iterate and check that what you're developing
matches the expectations.
"""
import math
import random

import pytest

from exercises.b_unit_test_demo.distance_metrics_corrected import great_circle_distance

# These are helper functions to make the tests more clear.
# In a professional setting, you could test these too. Additionally, for the
# pytest framework, you could make them into "fixtures", but that would lead
# us too far from the topic of this workshop.
def random_latitude() -> float:
    return random.uniform(-90, 90)

def random_longitude() -> float:
    return random.uniform(-180, 180)


def test_trivial_great_circle_distance():
    """The distance from a point to itself is zero (regardless of the unit in
    which that distance is measured)."""


    # Most tests work with an Arrange-Act-Assert pattern, which I've made
    # explicit here. If you follow this pattern, many programmers will be able
    # to read and understand your tests quicker.
    
    # Arrange
    point = (random_latitude(), random_longitude())

    # Act
    result = great_circle_distance(*point, *point)

    # Assert
    assert result == 0


def test_great_circle_distance_is_commutative():
    """The order in which the two coordinate sets are provided does not change the output.

    In other words, the (great-circle) distance from A to B is the same as from B to A."""
    pointA = (random_latitude(), random_longitude())
    pointB = (random_latitude(), random_longitude())

    distance_AB = great_circle_distance(*pointA, *pointB)
    distance_BA = great_circle_distance(*pointB, *pointA)

    # This is a nicety of pytest. Reads almost like any other normal English
    # phrase, no? It's at least clearer than what it hides, which is that the
    # absolute difference between these values is less than some tiny value,
    # which is the only correct way with computers that have finite memory to
    # compare two floating point numbers.
    assert distance_AB == pytest.approx(distance_BA)

def test_nontrivial_great_circle_distance():
    """Using basic maths, we will compute distances along a circle and compare
    those to the results of the great-circle-distance function we're testing."""

    # Many people in the class will use GPS-coordinates from objects they've
    # Googled, like the Eiffel tower and the Atomium in Brussels. And indeed,
    # that will show the way the function was written is faulty. However, you
    # are relying on an external service, like Google Maps, to give you a
    # result which you, nor your colleagues can easily verify in the future.
    # They too will need to trust that the distance you've copied from that
    # service into this test file is correctly copied AND that the service
    # produced the right result. This isn't the right approach. The right
    # approach is to use the right abstractions and thus simplify the problem:
    # Imagine our planet Earth. What 's the distance from the Northpole to the
    # equator? No matter how you look at our Earth, you're likely thinking of a
    # circular projection, where the Northpole is on top, and the equator is
    # the horizontal line that halves the circle in two equal parts. When you
    # look at it like that, the distance between the Northpole and the equator,
    # 2 points which are very easy to define by the way (latitudes 90 and 0,
    # longitude is irrelevant), then that great-circle-distance calculation
    # boils down to a quarter of the circumference of the circular projection.
    # If you know the radius of that circular projection, then you can verify
    # it all. And this radius is that magic number, 6371, that you might have
    # seen in the function we're testing. So, an improved version of that
    # function would be to factor out the magic number, add it as a default
    # argument. That way, we can use the great circle distance function for
    # things other than planet Earth, like a basketball or Jupiter. Thus, we
    # strike two birds in one stone: we've improved the function we're testing
    # by removing hardcoded values, and we've applied the right abstraction to
    # the problem to be able to test the original function.

    point_on_northpole = (90, random_longitude())
    point_on_equator = (0, random_longitude())

    result = great_circle_distance(*point_on_equator, *point_on_northpole, radius=1)  # The radius = 1 is new here.

    circumference_of_unit_circle = 2*math.pi*1  # unit circle means "radius=1"
    assert result == pytest.approx(circumference_of_unit_circle / 4)
