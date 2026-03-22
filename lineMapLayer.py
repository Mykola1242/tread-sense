from kivy.graphics import Color, Line
from kivy.graphics.instructions import InstructionGroup
from kivy_garden.mapview import MapLayer


class LineMapLayer(MapLayer):
    def __init__(self, points=None, **kwargs):
        super().__init__(**kwargs)
        self.points = points or []
        self.line_group = InstructionGroup()
        self.canvas.add(self.line_group)

    def reposition(self):
        self.redraw()

    def set_points(self, points):
        self.points = points
        self.redraw()

    def redraw(self):
        self.line_group.clear()

        if not self.parent or len(self.points) < 2:
            return

        mapview = self.parent
        screen_points = []

        for lat, lon in self.points:
            x, y = mapview.get_window_xy_from(lat, lon, mapview.zoom)
            screen_points.extend([x, y])

        self.line_group.add(Color(0, 0, 1, 0.8))
        self.line_group.add(Line(points=screen_points, width=2))