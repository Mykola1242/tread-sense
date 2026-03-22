from marshmallow import Schema, fields

class AccelerometerSchema(Schema):
    x = fields.Integer()
    y = fields.Integer()
    z = fields.Integer()