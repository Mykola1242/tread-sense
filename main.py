import csv
import threading
import time
from typing import List, Dict, Optional

import requests
from kivy.app import App
from kivy.clock import Clock, mainthread
from kivy_garden.mapview import MapMarker, MapView

from lineMapLayer import LineMapLayer


class MapViewApp(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.csv_file = "road_data.csv"
        self.store_url = "http://127.0.0.1:8000/api/road-data"

        self.points: List[Dict] = []
        self.current_index = 0

        self.car_marker: Optional[MapMarker] = None
        self.pothole_markers = []
        self.bump_markers = []

        self.marked_potholes = set()
        self.marked_bumps = set()

        self.route_points = []
        self.line_layer = None

        self.gravity_baseline = 9.81
        self.pothole_threshold = 4.0
        self.bump_threshold = 2.0

        self.use_csv = True
        self.use_store = False

        self.store_poll_interval = 2.0
        self.store_thread_running = False

    def build(self):
        self.mapview = MapView(zoom=15, lat=50.4501, lon=30.5234)
        return self.mapview

    def on_start(self):
        if self.use_csv:
            self.load_data_from_csv(self.csv_file)

        if self.points:
            first_point = self.points[0]
            self.mapview.center_on(first_point["lat"], first_point["lon"])

            self.car_marker = MapMarker(
                lat=first_point["lat"],
                lon=first_point["lon"],
                source="car.png"
            )
            self.car_marker.size = (40, 40)
            self.mapview.add_marker(self.car_marker)

        self.line_layer = LineMapLayer()
        self.mapview.add_layer(self.line_layer)

        Clock.schedule_interval(self.update, 1)

        if self.use_store:
            self.start_store_polling()

    def load_data_from_csv(self, filename: str):
        self.points.clear()

        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    point = {
                        "timestamp": row.get("timestamp"),
                        "lat": float(row["lat"]),
                        "lon": float(row["lon"]),
                        "ax": float(row.get("ax", 0.0)),
                        "ay": float(row.get("ay", 0.0)),
                        "az": float(row.get("az", 0.0)),
                        "speed": float(row.get("speed", 0.0)),
                    }
                    self.points.append(point)
                except (ValueError, KeyError):
                    continue

    def update(self, *args):
        if not self.points:
            return

        if self.current_index >= len(self.points):
            return

        point = self.points[self.current_index]

        self.update_car_marker(point)

        self.route_points.append((point["lat"], point["lon"]))
        self.line_layer.set_points(self.route_points)

        self.check_road_quality(point)

        self.current_index += 1

    def check_road_quality(self, point: Dict):
        az = point["az"]
        delta_z = az - self.gravity_baseline

        coord_key = (round(point["lat"], 6), round(point["lon"], 6))

        if abs(delta_z) >= self.pothole_threshold:
            if coord_key not in self.marked_potholes:
                self.set_pothole_marker(point)
                self.marked_potholes.add(coord_key)

        elif abs(delta_z) >= self.bump_threshold:
            if coord_key not in self.marked_bumps:
                self.set_bump_marker(point)
                self.marked_bumps.add(coord_key)

    def update_car_marker(self, point: Dict):
        if self.car_marker is None:
            self.car_marker = MapMarker(
                lat=point["lat"],
                lon=point["lon"],
                source="car.png"
            )
            self.car_marker.size = (40, 40)
            self.mapview.add_marker(self.car_marker)
        else:
            self.car_marker.lat = point["lat"]
            self.car_marker.lon = point["lon"]

        self.mapview.center_on(point["lat"], point["lon"])

    def set_pothole_marker(self, point: Dict):
        marker = MapMarker(
            lat=point["lat"],
            lon=point["lon"],
            source="pothole.png"
        )
        marker.size = (35, 35)
        self.pothole_markers.append(marker)
        self.mapview.add_marker(marker)

    def set_bump_marker(self, point: Dict):
        marker = MapMarker(
            lat=point["lat"],
            lon=point["lon"],
            source="bump.png"
        )
        marker.size = (35, 35)
        self.bump_markers.append(marker)
        self.mapview.add_marker(marker)

    def start_store_polling(self):
        if self.store_thread_running:
            return

        self.store_thread_running = True
        thread = threading.Thread(target=self.poll_store_loop, daemon=True)
        thread.start()

    def poll_store_loop(self):
        while self.store_thread_running:
            try:
                response = requests.get(self.store_url, timeout=5)
                response.raise_for_status()
                data = response.json()
                self.update_points_from_store(data)
            except Exception:
                pass

            time.sleep(self.store_poll_interval)

    @mainthread
    def update_points_from_store(self, data: List[Dict]):
        if not isinstance(data, list):
            return

        normalized = []
        for row in data:
            try:
                normalized.append({
                    "timestamp": row.get("timestamp"),
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "ax": float(row.get("ax", 0.0)),
                    "ay": float(row.get("ay", 0.0)),
                    "az": float(row.get("az", 0.0)),
                    "speed": float(row.get("speed", 0.0)),
                })
            except (ValueError, KeyError, TypeError):
                continue

        if normalized:
            self.points.extend(normalized)

    def on_stop(self):
        self.store_thread_running = False


if __name__ == '__main__':
    MapViewApp().run()