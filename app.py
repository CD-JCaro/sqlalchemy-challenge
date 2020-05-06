from flask import Flask, jsonify

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import numpy as np
import datetime as dt

app = Flask(__name__)

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

@app.route('/')
def home():
    return( 
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/startDate<br/>"
        f"/api/v1.0/startDate/endDate<br/>"
        f"date format: yyyy-mm-dd"
        )

@app.route('/api/v1.0/precipitation')
def precip():
    session = Session(engine)

    data = session.query(Measurement.date, Measurement.prcp).all()
    
    session.close()

    results = []
    for date, precip in data:
        entry = {}
        entry[date] = precip
        results.append(entry)

    return jsonify(results)

@app.route('/api/v1.0/stations')
def stations():
    session = Session(engine)

    data = session.query(Station.name).all()

    session.close()

    results = list(np.ravel(data))
    return jsonify(results)

@app.route('/api/v1.0/tobs')
def tobs():
    session = Session(engine)
    
    latestEntry = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date
    yearAgo = dt.datetime.strptime(latestEntry, "%Y-%m-%d") - dt.timedelta(days=366)

    pop = session.query(Measurement.station, func.count(Measurement.station)).filter(Measurement.date >= yearAgo)\
    .group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()

    topStation = pop[0]
    data = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == topStation)\
        .filter(Measurement.date >= yearAgo).all()

    session.close()
    
    results = []
    for date, temp in data:
        entry = {}
        entry[date] = temp
        results.append(entry)
    
    return jsonify(results)


def DoWork(startDate, endDate = None):
    session = Session(engine)

    col = [Measurement.date, 
       func.min(Measurement.tobs), 
       func.max(Measurement.tobs), 
       func.avg(Measurement.tobs)]

    data = session.query(*col).filter(Measurement.date >= startDate)
    if(endDate is not None):
        data = data.filter(Measurement.date <= endDate)
    
    data = data.group_by(Measurement.date).all()

    session.close()

    results = []
    for date, min, max, avg in data:
        entry = {}
        entry['Date'] = date
        entry['TMIN'] = min
        entry['TMAX'] = max
        entry['TAVG'] = avg
        results.append(entry)
    return results

@app.route('/api/v1.0/<startDate>')
def startonly(startDate):    
    results = DoWork(startDate)

    return jsonify(results)


@app.route('/api/v1.0/<startDate>/<endDate>')
def daterange(startDate, endDate):    
    results = DoWork(startDate, endDate)

    return jsonify(results)

if __name__ == "__main__":
    app.run(debug = True)