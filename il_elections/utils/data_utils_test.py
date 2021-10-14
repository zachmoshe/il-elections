"""Unit tests for the data_utils module."""
import unittest

import geopandas as gpd
import numpy as np
from parameterized import parameterized, parameterized_class
import shapely.geometry
import shapely.ops

from il_elections.utils import data_utils

# pylint: disable=protected-access

_SQUARE_1x1_POLYGON = shapely.geometry.box(0, 0, 1, 1)
_CIRCLE_1_POLYGON = shapely.geometry.Point(0, 0).buffer(1)


def _assert_polygons_coverage(testcase, polygon, covering_polygons):
    testcase.assertTrue(polygon.difference(shapely.ops.unary_union(covering_polygons)).is_empty)

def _assert_polygons_dont_overlap(testcase, polygons):
    # pylint: disable=consider-using-enumerate
    for i in range(len(polygons)):
        for j in range(len(polygons)):
            if i != j:
                # In the case where the two polygons are adjacent, only the boundary line
                # will be returned as the intersection, hence not an empty polygon but with
                # area = 0.
                testcase.assertEqual(polygons[i].intersection(polygons[j]).area, 0.)

@parameterized_class(('polygon', 'grid_legnth'), (
    (_SQUARE_1x1_POLYGON, 0.5),  # A 1x1 square divided into 4 cells.
    (_SQUARE_1x1_POLYGON, 0.75),  # A 1x1 square divided with leftovers.
    (_SQUARE_1x1_POLYGON, 2.),  # grid_legnth is longer than the polygon.
    (_CIRCLE_1_POLYGON, 4.),  # A circle in a single cell.
    (_CIRCLE_1_POLYGON, 1.),  # A circle divided into 4 cells.
))
class GeneratingCoveringPolygonsByGridLengthTest(unittest.TestCase):
    polygon: shapely.geometry.Polygon
    grid_length: float = .5

    def _generate_polygons(self):
        return list(data_utils._generate_covering_polygons_grid_cells_by_grid_length(
            self.polygon, self.grid_length))

    def test_polygons_coverage(self):
        polygons = self._generate_polygons()
        _assert_polygons_coverage(self, self.polygon, polygons)

    def test_polygons_dont_overlap(self):
        polygons = self._generate_polygons()
        _assert_polygons_dont_overlap(self, polygons)

    def test_cells_area(self):
        for p in self._generate_polygons():
            self.assertAlmostEqual(p.area, self.grid_length ** 2)

@parameterized_class(('polygon', 'grid_size'), (
    (_SQUARE_1x1_POLYGON, 1),
    (_SQUARE_1x1_POLYGON, 4),
    (_CIRCLE_1_POLYGON, 1),
    (_CIRCLE_1_POLYGON, 4),
))
class GeneratingCoveringPolygonsByGridSizeTest(unittest.TestCase):
    polygon: shapely.geometry.Polygon
    grid_size: int

    def _generate_polygons(self):
        return list(data_utils._generate_covering_polygons_grid_cells_by_grid_size(
            self.polygon, self.grid_size))

    def test_polygons_coverage(self):
        polygons = self._generate_polygons()
        _assert_polygons_coverage(self, self.polygon, polygons)

    def test_polygons_dont_overlap(self):
        polygons = self._generate_polygons()
        _assert_polygons_dont_overlap(self, polygons)

    def test_matches_grid_size(self):
        polygons = self._generate_polygons()
        self.assertEqual(len(polygons), self.grid_size ** 2)


class GenerateGridTest(unittest.TestCase):
    def test_remove_empty_cells(self):
        # polygon is a union of two narrow strips on the left and bottom sides. Therefore, doesn't
        # touch the upper-right quarter.
        polygon = shapely.ops.unary_union((
            shapely.geometry.box(0, 0, 0.1, 1),
            shapely.geometry.box(0, 0, 1, 0.1)))
        grid = data_utils._generate_grid(
            polygon,
            list(data_utils._generate_covering_polygons_grid_cells_by_grid_length(polygon, 0.5)))

        # the top right cell should be removed as it doesn't intersect with the polygon
        self.assertEqual(grid.shape[0], 3)

    @parameterized.expand((
        ('single_cell', _CIRCLE_1_POLYGON, 1),
        ('multi_cells', _CIRCLE_1_POLYGON, 2),
        ('multi_cells_with_empties', _CIRCLE_1_POLYGON, 100),
    ))
    def test_covers_the_polygon(self, _, polygon, grid_size):
        grid = data_utils._generate_grid(
            polygon,
            list(data_utils._generate_covering_polygons_grid_cells_by_grid_size(
                polygon, grid_size)))

        # the top right cell should be removed as it doesn't intersect with the polygon
        self.assertTrue(polygon.difference(grid.unary_union).is_empty)


class GroupPointsByPolygonTest(unittest.TestCase):
    def test_all_points_in_a_single_polygon(self):
        points = np.array([(1,1), (1,2), (2,1), (2,2)])
        points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(*points.T), crs=None)
        polygons = data_utils.generate_grid_by_size(shapely.geometry.box(0, 0, 3, 3), 1, crs=None)

        result = data_utils.group_points_by_polygons(points, polygons).size()

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0], 4)  # All 4 points

    def test_all_points_in_multiple_polygons(self):
        points = np.array([(1,1), (1,2), (2,1), (2,2)])
        points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(*points.T), crs=None)
        polygons = data_utils.generate_grid_by_size(shapely.geometry.box(0, 0, 3, 3), 2, crs=None)

        result = data_utils.group_points_by_polygons(points, polygons).size()

        self.assertEqual(len(result), 4)
        self.assertEqual(result.values.tolist(), [1, 1, 1, 1])

    def test_some_polygons_without_points(self):
        points = np.array([(1,1), (1,2), (2,1)])
        points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(*points.T), crs=None)
        polygons = data_utils.generate_grid_by_size(shapely.geometry.box(0, 0, 3, 3), 2, crs=None)

        result = data_utils.group_points_by_polygons(points, polygons).size()

        self.assertEqual(len(result), 3)
        self.assertEqual(result.values.tolist(), [1, 1, 1])

    def test_some_points_without_polygon(self):
        points = np.array([(1,1), (1,2), (2,1), (2,2), (10,10)])
        points = gpd.GeoDataFrame(geometry=gpd.points_from_xy(*points.T), crs=None)
        polygons = data_utils.generate_grid_by_size(shapely.geometry.box(0, 0, 3, 3), 2, crs=None)

        result = data_utils.group_points_by_polygons(points, polygons).size()

        self.assertEqual(len(result), 4)
        self.assertEqual(result.values.tolist(), [1, 1, 1, 1])
