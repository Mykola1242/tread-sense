from csv import reader
from datetime import datetime
from domain.aggregated_data import AggregatedData
from domain.accelerometer import Accelerometer 
from domain.gps import Gps
from domain.parking import Parking 

class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str) -> None: 
        self.accelerometer_filename = accelerometer_filename 
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        
        self.accel_file = None
        self.gps_file = None
        self.parking_file = None
        
        self.accel_reader = None
        self.gps_reader = None
        self.parking_reader = None

    def startReading(self, *args, **kwargs):
        self.accel_file = open(self.accelerometer_filename, 'r') 
        self.gps_file = open(self.gps_filename, 'r') 
        self.parking_file = open(self.parking_filename, 'r')
        
        self.accel_reader = reader(self.accel_file)
        self.gps_reader = reader(self.gps_file)
        self.parking_reader = reader(self.parking_file)
        
        for r in [self.accel_reader, self.gps_reader, self.parking_reader]:
            next(r, None)

    def read(self) -> AggregatedData:
        try:
            accel_data = next(self.accel_reader)
            gps_data = next(self.gps_reader)
            parking_data = next(self.parking_reader)
        except StopIteration:
            for f in [self.accel_file, self.gps_file, self.parking_file]:
                f.seek(0)
            
            self.accel_reader = reader(self.accel_file)
            self.gps_reader = reader(self.gps_file)
            self.parking_reader = reader(self.parking_file)
            
            for r in [self.accel_reader, self.gps_reader, self.parking_reader]:
                next(r, None) 
            
            accel_data = next(self.accel_reader)
            gps_data = next(self.gps_reader)
            parking_data = next(self.parking_reader)

        accelerometer = Accelerometer(int(accel_data[0]), int(accel_data[1]), int(accel_data[2]))
        gps = Gps(float(gps_data[0]), float(gps_data[1]))
        parking = Parking(
            empty_count=int(parking_data[0]), 
            gps=Gps(float(parking_data[1]), float(parking_data[2]))
        )

        return AggregatedData(
            accelerometer=accelerometer,
            gps=gps,
            parking=parking,
            time=datetime.now()
        )

    def stopReading(self, *args, **kwargs):
        for f in [self.accel_file, self.gps_file, self.parking_file]:
            if f: f.close()