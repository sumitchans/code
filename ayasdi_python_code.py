cd"""
Assignment Module for SymphonyAI - Python Coding Assignment
"""

import csv
import random
import datetime
import sqlite3
import itertools
import string


def get_data(column_data_type, is_null):
    """
    Function to get data for the column based on the column data type
    :param column_data_type: TEXT/DATE/INTEGER
    :param is_null: True/False
    :return: Date/int/str
    """
    col_val = None
    if column_data_type == "DATE":
        start_date = datetime.datetime.strptime("2014-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        col_val = datetime.datetime.strftime(
            start_date + datetime.timedelta(days=random.randrange(0, 365)), "%Y-%m-%d %H:%M:%S")
    elif column_data_type == "TEXT" and not is_null:
        _st = random.choices(string.ascii_lowercase + string.ascii_uppercase, k=5)
        col_val = "".join(_st)
    elif column_data_type == "INTEGER" and not is_null:
        col_val = random.randint(1000, 2000)
    return col_val


class SqlLite:
    """
    Context Manager to make connection with Sqlite database
    """
    def __init__(self, file_name):
        self.conn = sqlite3.connect(file_name)

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


class DataLoader:
    """
    Class to prepare data and load data in the Sqlite database
    """
    def __init__(self, name, columns_data, total_rows):
        self.file_name = name
        self.columns_meta = columns_data
        self.total_rows = total_rows

    def prepare_csv(self):
        """
        Function to prepare data and add in the csv file
        :return: None
        """
        _heading = self.columns_meta.keys()
        with open(self.file_name, "w+", newline="") as file:
            csv_writer = csv.writer(file, delimiter=',')
            csv_writer.writerow(_heading)
            row_batch_count = 10000
            rows_data = []
            for i in range(1, self.total_rows+1):
                _data = [str(i)]
                for key, value in columns_meta.items():
                    if key != "col1":
                        is_null = bool(i % 10 == 0)
                        col_val = get_data(value["TYPE"], is_null)
                        _data.append(col_val)
                rows_data.append(_data)
                if i % row_batch_count == 0:
                    csv_writer.writerows(rows_data)
                    rows_data = []

    @staticmethod
    def get_csv_data(file_obj):
        """
        Function to get csv data in chunks
        :param file_obj: csv file object
        :return: data
        """
        while True:
            _data = list(itertools.islice(file_obj, 1000))
            if not _data:
                break
            yield _data

    def load_data(self):
        """
        Function to load the data in the Sqlite database
        :return: None
        """
        with SqlLite("ayasdi_assignment.db") as conn:
            cursor = conn.cursor()
            try:
                table_col = [str(key) + " " + str(value["TYPE"] if value["TYPE"] != "DATE" else "TEXT")
                             + " " + ("" if value["ALLOW_NULL"] else "NOT NULL")
                             for key, value in self.columns_meta.items()]
                create_table_query = "CREATE TABLE AYASDI({data})".format(data=",".join(table_col))
                cursor.execute(create_table_query)
                conn.commit()
            except sqlite3.OperationalError:
                pass
            i = 0
            with open(self.file_name, "r+") as file:
                for csv_data in self.get_csv_data(file):
                    if i == 0:
                        columns = csv_data.pop(0)
                    i = i+len(csv_data)
                    query_data = []
                    for _d in csv_data:
                        row_data = "("
                        for val in _d.split(","):
                            if val.isdigit():
                                row_data = row_data + str(val)
                            else:
                                row_data = row_data + "'" + str(val) + "'"
                            row_data = row_data + ","
                        row_data = row_data[:-1] + ")"
                        query_data.append(row_data)
                    query = """INSERT INTO AYASDI({columns}) VALUES {values}""".format(
                        columns=columns,
                        values=",".join(query_data))
                    cursor.execute(query)
                conn.commit()


if __name__ == "__main__":
    columns_meta = dict(
        col1={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_2={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_3={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_4={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_5={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_6={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_7={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_8={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_9={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col_10={"TYPE": "INTEGER", "ALLOW_NULL": True},
        col11={"TYPE": "TEXT", "ALLOW_NULL": True},
        col12={"TYPE": "TEXT", "ALLOW_NULL": True},
        col13={"TYPE": "TEXT", "ALLOW_NULL": True},
        col14={"TYPE": "TEXT", "ALLOW_NULL": True},
        col15={"TYPE": "TEXT", "ALLOW_NULL": True},
        col16={"TYPE": "TEXT", "ALLOW_NULL": True},
        col17={"TYPE": "TEXT", "ALLOW_NULL": True},
        col18={"TYPE": "TEXT", "ALLOW_NULL": True},
        col19={"TYPE": "TEXT", "ALLOW_NULL": True},
        col20={"TYPE": "DATE", "ALLOW_NULL": False})
    ROW_COUNT = 1000000
    FILE_NAME = "data.csv"
    data_loader = DataLoader(FILE_NAME, columns_meta, ROW_COUNT)
    data_loader.prepare_csv()
    data_loader.load_data()
    print("completed")
