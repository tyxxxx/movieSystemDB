import sys
from utils.RowElement import RowElement
from utils.util import clear_temp_files
from .base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE, FIELD_PRINT_LEN, TEMP_DIR
import os
import re
import operator
import csv
from queue import PriorityQueue

class Relational(BaseEngine):
    def __init__(self):
        super().__init__()

    def run(self):
        print("Relational Database selected")
        while True:
            input_str = input("your query>").strip()
            if not self.parse_and_execute(input_str):
                break

    # ========================================================
    #              ***** Query Operations *****
    # ========================================================

    def show_tables(self, io_output=sys.stdout) -> bool:
        # print the header
        header_schema = ("tables",)
        format_str = self._get_format_str(header_schema, FIELD_PRINT_LEN)
        self._print_table_header(header_schema, format_str, io_output=io_output)
        # list the name of directories in Storage/Relational
        for file in os.listdir(f"{BASE_DIR}/Storage/Relational"):
            if os.path.isdir(f"{BASE_DIR}/Storage/Relational/{file}"):
                self._print_row({"tables": file}, header_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        return True

    def create_table(self, table_name, fields, io_output=sys.stdout) -> bool:
        if self._table_exists(table_name):
            print(f"Cannot create table. Table {table_name} already exists!", file=io_output)
            return True
        table_schema = tuple(fields)
        table_storage_path = self._get_table_path(table_name)
        # create the table directory
        os.mkdir(table_storage_path)
        # create the schema.txt
        with open(f"{table_storage_path}/schema.txt", "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(table_schema)
        print("table created", file=io_output)
        return True

    def drop_table(self, table_name, io_output=sys.stdout) -> bool:
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_storage_path = self._get_table_path(table_name)
        # delete the table directory
        for file in os.listdir(table_storage_path):
            os.remove(f"{table_storage_path}/{file}")
        os.rmdir(table_storage_path)
        print("table dropped", file=io_output)
        return True
    
    def load_data(self, file_name, io_output=sys.stdout) -> bool:
        # check if file is csv
        if not file_name.endswith(".csv"):
            print(f"File {file_name} is not a csv file", file=io_output)
            return True
        print("loading...")
        csv_file_path = f"{BASE_DIR}/ToBeLoaded/{file_name}"
        table_name = file_name.split(".")[0]
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        # create the table directory if not exists
        if not os.path.exists(table_storage_path):
            os.mkdir(table_storage_path)
        else:
            print("Cannot load dataset. Table already exists!", file=io_output)
            return True
        # read the first line of the csv to find the schema
        with open(csv_file_path, "r") as f:
            csv_reader = csv.reader(f)
            table_schema = next(csv_reader)
            # write the schema to the schema.txt
            with open(f"{table_storage_path}/schema.txt", "w") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(table_schema)
        
        # load the rest of the data to the storage using _insert_row
        with open(csv_file_path, "r") as f:
            csv_reader = csv.reader(f)
            next(csv_reader) # skip the first line
            for row in csv_reader:
                self._insert_row(table_name, row)
        print("loading succeeded", file=io_output)
        return True

    def insert_data(self, table_name: str, data: list, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        # get the table schema
        table_schema = self._get_table_schema(table_name)
        # check if the data is valid and convert the data to a dict
        data_dict = {}
        for field_data in data:
            # split the field_data into field_name and field_value
            field_name, field_value = field_data.split("=")
            # check if the field exists
            if not self._field_exists_in_schema(table_schema, field_name):
                print(f"Field {field_name} does not exist.", file=io_output)
                return True
            data_dict[field_name] = field_value
        # build the new row to be inserted
        row = self._dict_to_row(table_schema, data_dict)
        # insert the new row
        self._insert_row(table_name, row)
        print("insertion succeeded", file=io_output)
        return True

    def delete_data(self, table_name: str, condition: str, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # iterate through all chunks and delete rows that meet the condition
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r+") as c:
                csv_reader = csv.reader(c)
                typed_rows = self._read_typed_rows(table_types, csv_reader)
                # clear the file for replacement
                c.truncate(0)
            with open(chunk, "w") as c:
                csv_writer = csv.writer(c)
                for typed_row in typed_rows:
                    # leave the rows that are not supposed to be deleted
                    if not self._row_meets_condition(table_schema, typed_row, condition):
                        csv_writer.writerow(typed_row)
        print("deletion succeeded", file=io_output)
        return True
                            
    def update_data(self, table_name: str, condition: str, data: list, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # iterate through all chunks and update rows that meet the condition
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r+") as c:
                csv_reader = csv.reader(c)
                typed_rows = self._read_typed_rows(table_types, csv_reader)
                # clear the file for replacement
                c.truncate(0)
            with open(chunk, "w") as c:
                csv_writer = csv.writer(c)
                for typed_row in typed_rows:
                    # if meets the condition, update the row
                    if self._row_meets_condition(table_schema, typed_row, condition):
                        # dict containing old values
                        data_dict = self._row_to_dict(table_schema, typed_row)
                        # update the values in data_dict
                        for field_data in data:
                            field_name, field_value = field_data.split("=")
                            field_value = self._convert_to_type(field_value, self._get_field_type_from_types(table_schema, table_types, field_name))
                            data_dict[field_name] = field_value
                        # build the new row
                        new_row = self._dict_to_row(table_schema, data_dict)
                        # insert the new row
                        csv_writer.writerow(new_row)
                    else:
                        csv_writer.writerow(typed_row)
        print("update succeeded", file=io_output)
        return True

    def projection(self, table_name: str, fields: list, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        # create a schema for the projection table
        projection_schema = []
        if fields == ['*']:
            projection_schema = table_schema
        else:
            # check if the fields are in the table schema
            for field in fields:
                if not self._field_exists_in_schema(table_schema, field):
                    print(f"Field {field} does not exist.", file=io_output)
                    return True
            # add the fields to the projection schema
            for field in fields:
                projection_schema.append(field)
        # get the format string for printing
        format_str = self._get_format_str(projection_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(projection_schema, format_str, io_output=io_output)
        # iterate through all chunks and print the specified fields to console
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r") as c:
                csv_reader = csv.reader(c)
                for row in csv_reader:
                    row_dict = self._row_to_dict(table_schema, row)
                    # print the row
                    self._print_row(row_dict, projection_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        print("selection succeeded", file=io_output)
        return True

    def filtering(self, table_name: str, fields: list, condition: str, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        projection_schema = []
        if fields == ['*']:
            projection_schema = table_schema
        else:
            # check if the fields are in the table schema
            for field in fields:
                if not self._field_exists_in_schema(table_schema, field):
                    print(f"Field {field} does not exist.", file=io_output)
                    return True
            # create a schema for the projection table
            for field in fields:
                projection_schema.append(field)
        # get the format string for printing
        format_str = self._get_format_str(projection_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(projection_schema, format_str, io_output=io_output)
        # iterate through all chunks and print the specified fields to console
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r") as c:
                csv_reader = csv.reader(c)
                typed_rows = self._read_typed_rows(table_types, csv_reader)
                for typed_row in typed_rows:
                    # skip the rows that do not meet the condition
                    if not self._row_meets_condition(table_schema, typed_row, condition):
                        continue
                    row_dict = self._row_to_dict(table_schema, typed_row)
                    # print the row
                    self._print_row(row_dict, projection_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        print("filtering succeeded", file=io_output)
        return True


    def order(self, table_name: str, field: str, order_method: str, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        # check if the field is in the table schema
        table_schema = self._get_table_schema(table_name)
        if field not in table_schema:
            print(f"field {field} not in table schema", file=io_output)
            return True
        # do external sorting
        temp_sorted_file = self._external_sort(table_name, field, order_method)
        # print the merged file
        format_str = self._get_format_str(table_schema, FIELD_PRINT_LEN)
        self._print_table_header(table_schema, format_str, io_output=io_output)
        with open(temp_sorted_file, "r") as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                row_dict = {}
                for field in table_schema:
                    row_dict[field] = row[table_schema.index(field)]
                # print the row
                self._print_row(row_dict, table_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        # clear the Temp directory
        clear_temp_files()
        print("sorting succeeded", file=io_output)
        return True

    # Using Nested Loop Join
    def join(self, left: str, right: str, condition: str, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(left):
            print(f"Table {left} does not exist!", file=io_output)
            return True
        if not self._table_exists(right):
            print(f"Table {right} does not exist!", file=io_output)
            return True
        left_schema = self._get_table_schema(left)
        left_types = self._get_table_types(left)
        right_schema = self._get_table_schema(right)
        right_types = self._get_table_types(right)
        # extract the fields from the condition
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        if match is None:
            print(f"invalid condition {condition}", file=io_output)
            return True
        left_field, op, right_field = match.groups()
        # check if these fields exist
        if not self._field_exists_in_schema(left_schema, left_field):
            print(f"Field {left_field} does not exist.", file=io_output)
            return True
        if not self._field_exists_in_schema(right_schema, right_field):
            print(f"Field {right_field} does not exist.", file=io_output)
            return True
        # check if the types of the fields are the same
        left_field_type = self._get_field_type_from_types(left_schema, left_types, left_field)
        right_field_type = self._get_field_type_from_types(right_schema, right_types, right_field)
        if left_field_type != right_field_type:
            print(f"field {left_field} and field {right_field} have different types", file=io_output)
            return True
        # joined schema
        joined_schema = []
        for field in left_schema:
            joined_schema.append(f"{left}.{field}")
        for field in right_schema:
            joined_schema.append(f"{right}.{field}")
        joined_schema = tuple(joined_schema)
        # get the format string for printing
        format_str = self._get_format_str(joined_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(joined_schema, format_str, io_output=io_output)
        # for each chunk in the right table, iterate through all rows in the right table
        # and output matching rows to console
        # * we choose right table as the outter table because using the left table as the outter table
        # * will cause new condition to have reversed operator than the one user specified
        for right_chunk in self._get_table_chunks(right):
            with open(right_chunk, "r") as right_c:
                right_csv_reader = csv.reader(right_c)
                typed_right_rows = self._read_typed_rows(right_types, right_csv_reader)
                for typed_right_row in typed_right_rows:
                    right_field_value = self._get_row_value(right_schema, typed_right_row, right_field)
                    # convert the condition id=id to id=4 for the left table
                    new_condition = f"{left_field}{op}{right_field_value}"
                    # loop through inner table
                    for left_chunk in self._get_table_chunks(left):
                        with open(left_chunk, "r") as left_c:
                            left_csv_reader = csv.reader(left_c)
                            typed_left_rows = self._read_typed_rows(left_types, left_csv_reader)
                            for typed_left_row in typed_left_rows:
                                # check if the row meets the condition
                                if not self._row_meets_condition(left_schema, typed_left_row, new_condition):
                                    continue
                                # print the row
                                row_dict = {}
                                for field in left_schema:
                                    row_dict[f"{left}.{field}"] = self._get_row_value(left_schema, typed_left_row, field)
                                for field in right_schema:
                                    row_dict[f"{right}.{field}"] = self._get_row_value(right_schema, typed_right_row, field)
                                self._print_row(row_dict, joined_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        print("join succeeded", file=io_output)
        return True

    def aggregate(self, table_name, aggregate_method, aggregate_field, group_by_field, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # check if the fields are in the table schema
        if not self._field_exists_in_schema(table_schema, group_by_field):
            print(f"Field {group_by_field} does not exist.", file=io_output)
            return True
        if not self._field_exists_in_schema(table_schema, aggregate_field):
            print(f"Field {aggregate_field} does not exist.", file=io_output)
            return True
        # sort the table by group_by_field
        temp_sorted_file = self._external_sort(table_name, group_by_field, "asc")
        # output schema
        output_schema = (group_by_field, f"{aggregate_method}({aggregate_field})")
        # get the format string for printing
        format_str = self._get_format_str(output_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(output_schema, format_str, io_output=io_output)
        # iterate through the sorted table and output the aggregate result
        with open(temp_sorted_file, "r") as f:
            csv_reader = csv.reader(f)
            typed_row = self._next_typed_row(table_types, csv_reader)
            cur_group_result = None
            pre_group_by_field_value = None
            while typed_row is not None:
                # get the group_by_field value of the current typed row
                cur_group_by_field_value = self._get_row_value(table_schema, typed_row, group_by_field)
                # if the group_by_field value changes, output the aggregate result of the previous group
                if cur_group_by_field_value != pre_group_by_field_value and pre_group_by_field_value is not None:
                    if cur_group_result is not None and aggregate_method == "avg":
                        cur_group_result = round(cur_group_result[0] / cur_group_result[1], 2)
                    if cur_group_result is None:
                        cur_group_result = "0"
                    self._print_row({group_by_field: pre_group_by_field_value, f"{aggregate_method}({aggregate_field})": str(cur_group_result)}, output_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
                    # reset the aggregate result
                    cur_group_result = None
                # if the current row is in the same group as the previous row, update the aggregate result
                cur_aggregate_field_value = self._get_row_value(table_schema, typed_row, aggregate_field)
                if aggregate_method == "sum":
                    if cur_group_result is None:
                        cur_group_result = 0
                    cur_group_result += cur_aggregate_field_value
                elif aggregate_method == "avg":
                    if cur_group_result is None:
                        cur_group_result = [0, 0]
                    cur_group_result[0] += cur_aggregate_field_value
                    cur_group_result[1] += 1
                elif aggregate_method == "count":
                    if cur_group_result is None:
                        cur_group_result = 0
                    cur_group_result += 1
                elif aggregate_method == "max":
                    if cur_group_result is None:
                        cur_group_result = cur_aggregate_field_value
                    cur_group_result = max(cur_group_result, cur_aggregate_field_value)
                elif aggregate_method == "min":
                    if cur_group_result is None:
                        cur_group_result = cur_aggregate_field_value
                    cur_group_result = min(cur_group_result, cur_aggregate_field_value)
                # get the next row
                typed_row = self._next_typed_row(table_types, csv_reader)
                # update the prev_group_by_field_value
                pre_group_by_field_value = cur_group_by_field_value
            # output the aggregate result of the last group
            if typed_row is None and pre_group_by_field_value is not None:
                if cur_group_result is not None and aggregate_method == "avg":
                    cur_group_result = round(cur_group_result[0] / cur_group_result[1], 2)
                if cur_group_result is None:
                    cur_group_result = "0"
                self._print_row({group_by_field: pre_group_by_field_value, f"{aggregate_method}({aggregate_field})": str(cur_group_result)}, output_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        clear_temp_files()
        print("aggregate succeeded", file=io_output)
        return True

    def aggregate_table(self, table_name, aggregate_method, aggregate_field, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # check if the fields are in the table schema
        if not self._field_exists_in_schema(table_schema, aggregate_field):
            print(f"Field {aggregate_field} does not exist.", file=io_output)
            return True
        # output schema
        output_schema = (f"{aggregate_method}({aggregate_field})",)
        # get the format string for printing
        format_str = self._get_format_str(output_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(output_schema, format_str, io_output=io_output)
        # iterate through all chunks and output the aggregate result
        cur_result = None
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r") as c:
                csv_reader = csv.reader(c)
                typed_rows = self._read_typed_rows(table_types, csv_reader)
                for typed_row in typed_rows:
                    # get the aggregate_field value
                    cur_aggregate_field_value = self._get_row_value(table_schema, typed_row, aggregate_field)
                    if aggregate_method == "sum":
                        if cur_result is None:
                            cur_result = 0
                        cur_result += cur_aggregate_field_value
                    elif aggregate_method == "avg":
                        if cur_result is None:
                            cur_result = (0, 0)
                        cur_result = (cur_result[0] + cur_aggregate_field_value, cur_result[1] + 1)
                    elif aggregate_method == "count":
                        if cur_result is None:
                            cur_result = 0
                        cur_result += 1
                    elif aggregate_method == "max":
                        if cur_result is None:
                            cur_result = cur_aggregate_field_value
                        cur_result = max(cur_result, cur_aggregate_field_value)
                    elif aggregate_method == "min":
                        if cur_result is None:
                            cur_result = cur_aggregate_field_value
                        cur_result = min(cur_result, cur_aggregate_field_value)
        if aggregate_method == "avg" and cur_result is not None:
            cur_result = round(cur_result[0] / cur_result[1], 2)
        if cur_result is None:
            cur_result = "0"
        self._print_row({f"{aggregate_method}({aggregate_field})": str(cur_result)}, output_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        print("aggregate succeeded", file=io_output)
        return True

    def group(self, table_name, group_by_field, io_output=sys.stdout) -> bool:
        # check if the table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # check if the fields are in the table schema
        if not self._field_exists_in_schema(table_schema, group_by_field):
            print(f"Field {group_by_field} does not exist.", file=io_output)
            return True
        # sort the table by group_by_field
        temp_sorted_file = self._external_sort(table_name, group_by_field, "asc")
        # output schema
        output_schema = (group_by_field,)
        # get the format string for printing
        format_str = self._get_format_str(output_schema, FIELD_PRINT_LEN)
        # print the header
        self._print_table_header(output_schema, format_str, io_output=io_output)
        # iterate through the sorted table and output the aggregate result
        with open(temp_sorted_file, "r") as f:
            csv_reader = csv.reader(f)
            typed_row = self._next_typed_row(table_types, csv_reader)
            pre_group_by_field_value = None
            while typed_row is not None:
                # get the group_by_field value of the current typed row
                cur_group_by_field_value = self._get_row_value(table_schema, typed_row, group_by_field)
                # if the group_by_field value changes, output the aggregate result of the previous group
                if cur_group_by_field_value != pre_group_by_field_value and pre_group_by_field_value is not None:
                    self._print_row({group_by_field: pre_group_by_field_value}, output_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
                # get the next row
                typed_row = self._next_typed_row(table_types, csv_reader)
                # update the prev_group_by_field_value
                pre_group_by_field_value = cur_group_by_field_value
            # output the aggregate result of the last group
            if typed_row is None and pre_group_by_field_value is not None:
                self._print_row({group_by_field: pre_group_by_field_value}, output_schema, format_str, FIELD_PRINT_LEN, io_output=io_output)
        clear_temp_files()
        print("group succeeded", file=io_output)
        return True

    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For value typping 
    # ========================================================

    # refrence the types of the table based on this row and write the types to the schema.txt
    def _type_reference_from_row(self, table_name: str, row: list) -> None:
        schema_path = f"{BASE_DIR}/Storage/Relational/{table_name}/schema.txt"
        schema = self._get_table_schema(table_name)
        # check if row has the same number of fields as the schema
        if len(row) != len(schema):
            raise Exception(f"row does not match the schema")
        # generate a tuple of types based on the row values
        types = []
        for field in schema:
            field_index = schema.index(field)
            field_value = row[field_index]
            if field_value.isdigit():
                types.append(int)
            elif field_value.replace('.', '', 1).isdigit():
                types.append(float)
            else:
                types.append(str)
        types = tuple(types)
        # write the types to the schema.txt
        with open(schema_path, "a") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(types)
    
    # convert the value to the type
    def _convert_to_type(self, value: str, type: type) -> int or float or str:
        if type == int:
            return int(value) if value != "" else 0
        elif type == float:
            return float(value) if value != "" else float("0.0")
        else:
            return value
    
    # return the type of the field
    def _get_field_type_from_types(self, schema: tuple, types: tuple, field: str) -> int or float or str:
        return types[schema.index(field)]
    
    # return the type of the field
    def _get_field_type_from_table(self, table_name: str, field: str) -> int or float or str:
        schema = self._get_table_schema(table_name)
        types = self._get_table_types(table_name)
        return types[schema.index(field)]
    
    def _convert_row_to_typed_row(self, types: tuple, row: list) -> list:
        typed_row = []
        for i in range(len(row)):
            field_value = row[i]
            field_type = types[i]
            typed_row.append(self._convert_to_type(field_value, field_type))
        return typed_row
    
    def _next_typed_row(self, types: tuple, reader: csv.reader) -> list or None:
        row = next(reader, None)
        if row is None:
            return None
        return self._convert_row_to_typed_row(types, row)
    
    def _read_typed_rows(self, types: tuple, reader: csv.reader) -> list:
        rows = []
        row = next(reader, None)
        while row is not None:
            rows.append(self._convert_row_to_typed_row(types, row))
            row = next(reader, None)
        return rows

    # ========================================================
    #                  ***** Helpers *****
    #
    #                  For table management
    # ========================================================

    def _get_table_path(self, table_name: str) -> str:
        return f"{BASE_DIR}/Storage/Relational/{table_name}"

    def _table_exists(self, table_name: str) -> bool:
        table_storage_path = f"{BASE_DIR}/Storage/Relational/{table_name}"
        if not os.path.exists(table_storage_path):
            return False
        return True
        
    # return a tuple of field names
    def _get_table_schema(self, table_name: str) -> tuple:
        schema_path = f"{BASE_DIR}/Storage/Relational/{table_name}/schema.txt"
        with open(schema_path, "r") as f:
            csv_reader = csv.reader(f)
            schema = next(csv_reader)
        return tuple(schema)
    
    # return a tuple of types of the table
    def _get_table_types(self, table_name: str) -> tuple:
        schema_path = f"{BASE_DIR}/Storage/Relational/{table_name}/schema.txt"
        with open(schema_path, "r") as f:
            csv_reader = csv.reader(f)
            schema = next(csv_reader, [])
            types = next(csv_reader, [])
        if len(schema) != len(types):
            # get first row in chunk_0.csv
            table_storage_path = self._get_table_path(table_name)
            chunk_path = f"{table_storage_path}/chunk_0.csv"
            with open(chunk_path, "r") as f:
                csv_reader = csv.reader(f)
                row = next(csv_reader, None)
            if row is None:
                raise Exception(f"Table {table_name} is empty, cannot get types")
            # reference types from the first row
            self._type_reference_from_row(table_name, row)
            # get the types again
            with open(schema_path, "r") as f:
                csv_reader = csv.reader(f)
                schema = next(csv_reader, [])
                types = next(csv_reader, [])
        # deserialize the types to type objects
        for i in range(len(types)):
            if types[i] == "<class 'int'>":
                types[i] = int
            elif types[i] == "<class 'float'>":
                types[i] = float
            else:
                types[i] = str
        return tuple(types)
    
    # return a list of chunk paths
    def _get_table_chunks(self, table_name: str) -> list:
        table_storage_path = self._get_table_path(table_name)
        chunks = []
        for file in os.listdir(table_storage_path):
            if file.endswith(".csv"):
                chunks.append(f"{table_storage_path}/{file}")
        return chunks
    
    def _check_if_field_exists_in_table(self, table_name: str, field: str) -> None:
        table_schema = self._get_table_schema(table_name)
        if field not in table_schema:
            raise Exception(f"Field {field} does not exist in {table_name}!")
        
    def _field_exists_in_schema(self, schema: tuple, field: str) -> bool:
        if field not in schema:
            return False
        return True

    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For chunk naming
    # ========================================================

    def _get_chunk_number(self, chunk_path: str) -> int:
        # chunk name example: chunk_0.csv
        return int(chunk_path.split("/")[-1].split(".")[0].split("_")[-1])
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For row operations
    # ========================================================

    # Given schema and the data_dict that stores the (field_name, field_value) pairs,
    # build a row and return it
    def _dict_to_row(self, schema: tuple, data_dict: dict) -> list:
        row = []
        for field in schema:
            row.append(data_dict.get(field, ""))
        return row
    
    def _row_to_dict(self, schema: tuple, row: list) -> dict:
        row_dict = {}
        for field in schema:
            row_dict[field] = self._get_row_value(schema, row, field)
        return row_dict
    
    def _get_row_value(self, schema: tuple, row: list, field: str) -> int or float or str:
        return row[schema.index(field)]
    
    # insert the row into the table
    # Assumption: 
    # - the row is valid and matches the schema
    # - the table exists
    def _insert_row(self, table_name: str, row: list) -> None:
        table_storage_path = self._get_table_path(table_name)
        # iterate through all chunks in this directory and find the chunk_num with max num
        max_chunk_num = -1
        for chunk in self._get_table_chunks(table_name):
            chunk_num = self._get_chunk_number(chunk)
            if chunk_num > max_chunk_num:
                max_chunk_num = chunk_num
        # If the max_chunk_num is -1, there is no chunk already created. Create a new chunk.
        if max_chunk_num == -1:
            # create a new csv chunk
            with open(f"{table_storage_path}/chunk_0.csv", "w") as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(row)
                # reference types from the first row
                self._type_reference_from_row(table_name, row)
                # print a warning message
                warning_msg = """
*** Warning: The types of the table are referenced from 
*** the first row of the table. The first row cannot 
*** contain empty values
"""
                print(warning_msg)
        else:
            # check if the last chunk is full
            with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "r") as f:
                csv_reader = csv.reader(f)
                rows = list(csv_reader)
                if len(rows) < CHUNK_SIZE:
                    # last chunk is not full -> append to the last chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num}.csv", "a") as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow(row)
                else:
                    # lcat chunk is full -> create a new chunk
                    with open(f"{table_storage_path}/chunk_{max_chunk_num + 1}.csv", "w") as f:
                        csv_writer = csv.writer(f)
                        csv_writer.writerow(row)
        
    def _row_meets_condition(self, schema, typed_row, condition):
        # !!! issue: only support one condition
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        field, op, value = match.groups()

        field_type = type(self._get_row_value(schema, typed_row, field))
        value = self._convert_to_type(value, field_type)

        # get the operator function
        ops = {
            "=": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
        }
        op_func = ops.get(op)

        # check if the operator is valid
        if not op_func:
            raise Exception(f"Invalid operator {op}")
        
        # get the index of the field
        field_index = schema.index(field)
        # get the value of the field
        typed_row_value = typed_row[field_index]
        # compare the row_value and value
        return op_func(typed_row_value, value)
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For temporary files
    # ========================================================

    # for create temporary files name in external sort
    def _temp_file_name(self, chunk_num: int, pass_num: int) -> str:
        return f"{TEMP_DIR}/chunk_{chunk_num}_pass_{pass_num}.csv"

    def _get_chunk_number_from_temp_file(self, temp_file_name: str) -> int:
        return int(temp_file_name.split("/")[-1].split(".")[0].split("_")[1])
    
    def _get_pass_number_from_temp_file(self, temp_file_name: str) -> int:
        return int(temp_file_name.split("/")[-1].split(".")[0].split("_")[3])
    
    def _get_temp_chunks(self) -> list:
        temp_chunks = []
        for file in os.listdir(TEMP_DIR):
            if file.endswith(".csv"):
                temp_chunks.append(f"{TEMP_DIR}/{file}")
        return temp_chunks

    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For external sort
    # ========================================================

    def _external_sort(self, table_name: str, field: str, order_method: str) -> str:
        table_schema = self._get_table_schema(table_name)
        table_types = self._get_table_types(table_name)
        # sorting phase
        for chunk in self._get_table_chunks(table_name):
            with open(chunk, "r") as c:
                csv_reader = csv.reader(c)
                typed_rows = self._read_typed_rows(table_types, csv_reader)
                # sort the current chunk using STD sort
                cur_sorted_table = sorted(typed_rows, key = lambda typed_row: self._get_row_value(table_schema, typed_row, field), reverse = order_method == "desc")
            # write the sorted table to the Temp directory
            chunk_num = self._get_chunk_number(chunk)
            with open(self._temp_file_name(chunk_num, 0), "w") as c:
                csv_writer = csv.writer(c)
                csv_writer.writerows(cur_sorted_table)
        # merging phase
        return self._merge_sorted_chunks(field, table_schema, table_types, order_method, 0)

    def _merge_sorted_chunks(self, field, schema, types, order_method, pass_num) -> str:
        # find the max chunk number under the Temp directory
        max_chunk_num = -1
        for chunk in self._get_temp_chunks():
            # skip the files not in current pass
            chunk_pass_num = self._get_pass_number_from_temp_file(chunk)
            if chunk_pass_num != pass_num:
                continue
            chunk_num = self._get_chunk_number_from_temp_file(chunk)
            if chunk_num > max_chunk_num:
                max_chunk_num = chunk_num

        if max_chunk_num == -1:
            # no data in the Temp directory
            raise Exception("No data in the Temp directory")

        if max_chunk_num == 0:
            # this is the final run, no need to merge
            return self._temp_file_name(0, pass_num)
        
        next_chunk_num = 0 # the chunk number of the merged file in the next pass
        start_chunk_num = 0 # the starting chunk of current merge group
        end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1) # the ending chunk of current merge group

        while start_chunk_num <= max_chunk_num:
            output_file = self._temp_file_name(next_chunk_num, pass_num + 1)
            reader_dict = {}
            loaded_rows = PriorityQueue() # pq of RowElement
            # open the csv readers for all the chunks in the current merge group
            opened_files = []
            for cur_chunk_num in range(start_chunk_num, end_chunk_num):
                cur_chunk = self._temp_file_name(cur_chunk_num, pass_num)
                opened_file = open(cur_chunk, "r")
                opened_files.append(opened_file)
                reader_dict[cur_chunk_num] = csv.reader(opened_file)
            # load the first row from each chunk into the heap
            for cur_chunk_num in range(start_chunk_num, end_chunk_num):
                csv_reader = reader_dict[cur_chunk_num]
                typed_row = self._next_typed_row(types, csv_reader)
                if typed_row is not None:
                    row_element = RowElement(cur_chunk_num, typed_row, schema.index(field), order_method)
                    loaded_rows.put(row_element)
            # output until the heap is empty
            while not loaded_rows.empty():
                row_element = loaded_rows.get()
                typed_row = row_element.row
                chunk_num = row_element.chunk_num
                # write the row to the output file
                with open(output_file, "a") as f:
                    csv_writer = csv.writer(f)
                    csv_writer.writerow(typed_row)
                # load the next row from the same chunk that output the row
                next_row = self._next_typed_row(types, reader_dict[chunk_num])
                if next_row is not None:
                    row_element = RowElement(chunk_num, next_row, schema.index(field), order_method)
                    loaded_rows.put(row_element)
            # close all open files
            for opened_file in opened_files:
                opened_file.close()
            # update the start/end_chunk_num and next_chunk_num and proceed to the next merge group
            start_chunk_num += CHUNK_SIZE
            end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1)
            next_chunk_num += 1
        # proceed to the next pass
        return self._merge_sorted_chunks(field, schema, types, order_method, pass_num + 1)
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                  For printing tables
    # ========================================================

    def _get_format_str(self, schema, max_length=10):
        format_str = ""
        for field in schema:
            format_str += f"{{:<{max_length}}}"
        return format_str

    def _print_table_header(self, schema, format_str, io_output=sys.stdout):
        print("=" * len(format_str.format(*schema)), file=io_output)
        print(format_str.format(*schema), file=io_output)
        print("=" * len(format_str.format(*schema)), file=io_output)

    # max_length must be >= 6
    def _print_row(self, row_dict, schema, format_str, max_length, io_output=sys.stdout):
        row_list = []
        for field in schema:
            field_value = str(row_dict[field])
            field_value += "   "
            if len(field_value) > max_length:
                field_value = field_value[:max_length - 6] + "...   "
                row_list.append(field_value)
            else:
                row_list.append(field_value)
        print(format_str.format(*row_list), file=io_output)