import csv
import datetime
import sqlite3
import shutil


class Measure:
    def __init__(self, date, has_hour, weight):
        self.date = date
        self.has_hour = has_hour
        self.weight = weight


show_measures = False
measures_list = []

with open('C:\\coding\\WeigthTrackUnificaction\\original_data\\Peso - 2014.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif ~(row[0] == "") & ~(row[1] == "") & (len(row[0].split("/")) >= 2):
            date_parts = row[0].split("/")
            new_weight = row[1].replace(".", "")
            while len(new_weight) < 5:
                new_weight = new_weight + "0"
            new_measure = Measure(datetime.datetime(int(date_parts[2]), int(date_parts[0]), int(date_parts[1])),
                                  False,
                                  int(new_weight))
            if show_measures:
                (str(new_measure.date) + " ---- " + str(new_measure.weight))
            measures_list.append(new_measure)
            line_count += 1
        else:
            line_count += 1
    print(f'Processed {line_count} lines.')
print("I've catch " + str(len(measures_list)) + " records.")

with open('C:\\coding\\WeigthTrackUnificaction\\original_data\\Peso - 2015.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif ~(row[0] == "") & ~(row[1] == "") & (len(row[0].split("/")) >= 2):
            date_parts = row[0].split("/")
            new_weight = row[1].replace(".", "")
            while len(new_weight) < 5:
                new_weight = new_weight + "0"
            new_measure = Measure(datetime.datetime(int(date_parts[2]), int(date_parts[0]), int(date_parts[1])),
                                  False,
                                  int(new_weight))
            if show_measures:
                print(str(new_measure.date) + " ---- " + str(new_measure.weight))
            measures_list.append(new_measure)
            line_count += 1
        else:
            line_count += 1
    print(f'Processed {line_count} lines.')
print("I've catch " + str(len(measures_list)) + " records.")

with open('C:\\coding\\WeigthTrackUnificaction\\original_data\\Peso - Hoja 1.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif ~(row[0] == "") & ~(row[1] == "") & (len(row[0].split("/")) >= 2):
            date_parts = row[0].split("/")
            new_weight = row[1].replace(",", "")
            while len(new_weight) < 5:
                new_weight = new_weight + "0"
            new_measure = Measure(datetime.datetime(int(date_parts[2]), int(date_parts[1]), int(date_parts[0])),
                                  False,
                                  int(new_weight))
            if show_measures:
                print(str(new_measure.date) + " ---- " + str(new_measure.weight))
            measures_list.append(new_measure)
            line_count += 1
        else:
            line_count += 1
    print(f'Processed {line_count} lines.')
print("I've catch " + str(len(measures_list)) + " records.")

conn = sqlite3.connect("C:\\coding\\WeigthTrackUnificaction\\original_data\\BmiWeightTracker_20171213_1045.db")
c = conn.cursor()
c.execute("SELECT date, value FROM weight")
line_count = 0
for row in c.fetchall():
    date_parts = row[0].split(" ")[0].split("-")
    time_parts = row[0].split(" ")[1].split(":")
    new_weight = str(row[1]).replace(".", "")
    while len(new_weight) < 5:
        new_weight = new_weight + "0"
    new_measure = Measure(datetime.datetime(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]),
                                            int(time_parts[0]), int(time_parts[1])),
                          True,
                          int(new_weight))
    if show_measures:
        print(str(new_measure.date) + " ---- " + str(new_measure.weight))
    measures_list.append(new_measure)
    line_count += 1
print(f'Processed {line_count} lines.')
print("I've catch " + str(len(measures_list)) + " records.")

partial_measures_list = measures_list.copy()

conn = sqlite3.connect("C:\\coding\\WeigthTrackUnificaction\\original_data\\BmiWeightTracker_20190325_1331.db")
c = conn.cursor()
c.execute("SELECT date, value FROM weight")
line_count = 0
for row in c.fetchall():
    date_parts = row[0].split(" ")[0].split("-")
    time_parts = row[0].split(" ")[1].split(":")
    new_weight = str(row[1]).replace(".", "")
    while len(new_weight) < 5:
        new_weight = new_weight + "0"
    new_measure = Measure(datetime.datetime(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]),
                                            int(time_parts[0]), int(time_parts[1])),
                          True,
                          int(new_weight))
    if show_measures:
        print(str(new_measure.date) + " ---- " + str(new_measure.weight))
    measures_list.append(new_measure)
    line_count += 1
print(f'Processed {line_count} lines.')
print("I've catch " + str(len(measures_list)) + " records.")

# Crear *.csv
create_csv = True
if create_csv:
    with open("C:\\coding\\WeigthTrackUnificaction\\unified_data\\Peso20190327.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(["Date", "Weight", "Time"])
        for measure in measures_list:
            writable_date = str(measure.date.year) + "-" + str(measure.date.month) + "-" + str(measure.date.day)
            writable_time = (str(measure.date.hour) + ":" + str(measure.date.minute)) if measure.has_hour else "12:00"
            writer.writerow([writable_date, str(measure.weight), writable_time])

# Crear *.db
shutil.copyfile("C:\\coding\\WeigthTrackUnificaction\\original_data\\BmiWeightTracker_20190325_1331.db",
                "C:\\coding\\WeigthTrackUnificaction\\unified_data\\BmiWeightTracker_20190326_1331.db")
conn = sqlite3.connect("C:\\coding\\WeigthTrackUnificaction\\unified_data\\BmiWeightTracker_20190326_1331.db")
c = conn.cursor()
for measure in partial_measures_list:
    writable_date = str(measure.date.year) + "-" + str(measure.date.month) + "-" + str(measure.date.day)
    if measure.has_hour:
        writable_date = writable_date + str(measure.date.hour) + ":" + str(measure.date.minute)
    else:
        writable_date = writable_date + "12:00"
    writable_weight = float(measure.weight)/1000
    c.execute("INSERT INTO weight (date, value, profile_id) VALUES (?, ?, ?)",(writable_date, writable_weight, 1))
    conn.commit()
