#!/usr/bin/env python3

import json, os, sys, argparse, datetime
from collections import Counter, OrderedDict

from . import database

def process_time(unix_time):
    out = OrderedDict([("ut",None), ("iso", None), ("short", "")])
    if unix_time:
        dt = datetime.datetime.fromtimestamp(float(unix_time))
        out["ut"] = unix_time
        out["iso"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
        out["date"] = dt.strftime("%Y-%m-%d")
        out["short"] = dt.strftime("%H%M") + "½"*(dt.second==30)
    return out

def get_departure_board(location, datetime_from, duration):
    timestamp = datetime_from.timestamp()
    ret = []
    with database.DatabaseConnection() as db_connection, db_connection.new_cursor() as c:
        c.execute("""SELECT
            arrival_scheduled,departure_scheduled,pass_scheduled,
            ta.datetime_actual,td.datetime_actual,
            arrival_public, departure_public,
            platform, line, path, activity, engineering_allowance, pathing_allowance, performance_allowance,
            ta.actual_platform, ta.actual_line, ta.actual_route, ta.actual_variation_status, ta.actual_variation, ta.actual_direction, ta.actual_source,
            td.actual_platform, td.actual_line, td.actual_route, td.actual_variation_status, td.actual_variation, td.actual_direction, td.actual_source,

            flat_schedules.uid, category, signalling_id, headcode, power_type, timing_load, speed, operating_characteristics, seating_class, sleepers,
            reservations, catering, branding, uic_code, atoc_code,

            start_date, actual_signalling_id, trust_id, current_variation,

            l0.tiploc, l0.name, l0.stanox, l0.crs,
            l1.tiploc, l1.name, l1.stanox, l1.crs,
            l2.tiploc, l2.name, l2.stanox, l2.crs,
            l3.tiploc, l3.name, l3.stanox, l3.crs,
            l4.tiploc, l4.name, l4.stanox, l4.crs

            FROM flat_timing
            INNER JOIN schedule_locations ON schedule_location_iid=schedule_locations.iid
            INNER JOIN schedules ON schedule_locations.schedule_iid=schedules.iid
            INNER JOIN flat_schedules ON flat_timing.flat_schedule_iid=flat_schedules.iid

            INNER JOIN locations as l0 ON schedule_locations.location_iid=l0.iid
            INNER JOIN locations as l1 ON schedules.origin_location_iid=l1.iid
            INNER JOIN locations as l2 ON schedules.destination_location_iid=l2.iid
            LEFT JOIN locations as l3 ON flat_schedules.current_location=l3.iid
            LEFT JOIN locations as l4 ON flat_schedules.cancellation_location=l4.iid

            LEFT JOIN trust_movements as ta ON
            (ta.movement_type='A' AND flat_schedules.iid=ta.flat_schedule_iid AND arrival_scheduled=ta.datetime_scheduled)
            LEFT JOIN trust_movements as td ON
            (td.movement_type='D' AND flat_schedules.iid=td.flat_schedule_iid AND (departure_scheduled=td.datetime_scheduled OR pass_scheduled=td.datetime_scheduled))
            WHERE flat_timing.location_iid in (select iid from locations where %s in (crs, tiploc))
            AND departure_scheduled BETWEEN %s AND %s ORDER BY departure_scheduled;
            """, [location, timestamp, timestamp+60*duration])

        for row in c:
            row = list(row)
            out = OrderedDict()
            for tag in ["arrival_scheduled", "departure_scheduled", "pass_scheduled", "arrival_actual", "departure_actual"]:
                out[tag] = process_time(row.pop(0))

            for tag in ["arrival_public", "departure_public", "platform", "line", "path", "activity", "engineering_allowance", "pathing_allowance", "performance_allowance"]:
                out[tag] = row.pop(0)

            out["trust_arrival"] = OrderedDict([(tag,row.pop(0)) for tag in ["platform", "line", "route", "variation_status", "variation", "direction", "source"]])
            out["trust_departure"] = OrderedDict([(tag,row.pop(0)) for tag in ["platform", "line", "route", "variation_status", "variation", "direction", "source"]])

            out["service"], out["here"], out["origin"], out["destination"], out["last_location"], out["cancellation_location"] = OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict()

            for tag in ["uid", "category", "signalling_id", "headcode", "power_type", "timing_load", "speed", "operating_characteristics", "seating_class", "sleepers", "reservations", "catering", "branding", "uic_code", "atoc_code", "date", "actual_signalling_id", "trust_id", "current_variation"]:
                out["service"][tag] = row.pop(0)

            out["service"]["operating_characteristics"] = out["service"]["operating_characteristics"].rstrip()
            if out["platform"]:
                out["platform"] = out["platform"].rstrip()

            for first in ["here", "origin", "destination", "last_location", "cancellation_location"]:
               for second in ["tiploc", "name", "stanox", "crs"]:
                   out[first][second] = row.pop(0)

            ret.append(out)
        return ret
