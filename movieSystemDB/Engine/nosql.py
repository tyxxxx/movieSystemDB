import csv
import json
import operator
import os
from queue import PriorityQueue
import re
import sys
from Engine.base import BaseEngine
from config import BASE_DIR, CHUNK_SIZE, TEMP_DIR
from utils.DocElement import DocElement
from utils.util import add_key, clear_temp_files, get_key_val, mix_key

class NoSQL(BaseEngine):
    def __init__(self):
        super().__init__()
    
    def run(self):
        print("NoSQL Database selected")
        while True:
            input_str = input("your query>").strip()
            if not self.parse_and_execute(input_str):
                break

    def show_tables(self, io_output=sys.stdout) -> bool:
        for file in os.listdir(f"{BASE_DIR}/Storage/NoSQL"):
            if os.path.isdir(f"{BASE_DIR}/Storage/NoSQL/{file}"):
                self._print_doc({"table": file}, io_output=io_output)
        return True
    
    def create_table(self, table_name: str, fields: list, io_output=sys.stdout) -> bool:
        # check if table already exists
        if self._table_exists(table_name):
            print(f"Table {table_name} already exists!", file=io_output)
            return True
        if len(fields) != 0 and fields[0] != "":
            print("Warning: NoSQL does not support schema!", file=io_output)
        table_storage_path = self._get_table_path(table_name)
        # create the table directory
        os.mkdir(table_storage_path)
        print("table created", file=io_output)
        return True

    def drop_table(self, table_name: str, io_output=sys.stdout) -> bool:
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
            print("file must be a csv file", file=io_output)
            return True
        print("loading data...")
        csv_file_path = f"{BASE_DIR}/ToBeLoaded/{file_name}"
        table_name = file_name.split(".")[0]
        table_storage_path = f"{BASE_DIR}/Storage/NoSQL/{table_name}"
        # create the table directory if not exists
        if not os.path.exists(table_storage_path):
            os.mkdir(table_storage_path)
        else:
            print("Cannot load dataset. Table already exists!", file=io_output)
            return True
        # read the first line of the csv to find the schema
        with open(csv_file_path, 'r') as f:
            csv_reader = csv.reader(f)
            table_schema = next(csv_reader)
            csv_row = next(csv_reader, None)
            while csv_row is not None:
                # convert csv row to json
                doc = self._csv_row_to_doc(csv_row, table_schema)
                # insert the doc into the table
                self._insert_doc(table_name, doc)
                csv_row = next(csv_reader, None)
        print("loading succeeded", file=io_output)
        return True
    
    def insert_data(self, table_name: str, data: list, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        # convert the data to json
        doc = {}
        for field_data in data:
            field_name, field_value = field_data.split("=")
            # convert to correct type
            doc[field_name] = self._get_typed_value(field_value)
        # insert the doc into the table
        self._insert_doc(table_name, doc)
        print("insertion succeeded", file=io_output)
        return True
    
    def delete_data(self, table_name: str, condition: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            self._clear_file(chunk)
            filtered_docs = filter(lambda doc: not self._doc_meets_condition(doc, condition), docs)
            self._write_docs_to_file(filtered_docs, chunk)
        print("deletion succeeded", file=io_output)
        return True
    
    def update_data(self, table_name: str, condition: str, data: list, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            self._clear_file(chunk)
            for doc in docs:
                if self._doc_meets_condition(doc, condition):
                    for field_data in data:
                        field_name, field_value = field_data.split("=")
                        doc[field_name] = self._get_typed_value(field_value)
                self._write_doc_to_file(doc, chunk)
        print("update succeeded", file=io_output)
        return True
    
    def projection(self, table_name: str, fields: list, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            for doc in docs:
                projected_doc = {}
                if len(fields) == 1 and fields[0] == "*":
                    # if fields is *, return the whole doc
                    projected_doc = doc
                else:
                    # else, return only the fields in fields
                    for field in fields:
                        if field in doc:
                            projected_doc[field] = doc[field]
                self._print_doc(projected_doc, io_output=io_output)
        print("projection succeeded", file=io_output)
        return True
    
    def filtering(self, table_name: str, fields: list, condition: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            for doc in docs:
                if self._doc_meets_condition(doc, condition):
                    projected_doc = {}
                    if len(fields) == 1 and fields[0] == "*":
                        # if fields is *, return the whole doc
                        projected_doc = doc
                    else:
                        # else, return only the fields in fields
                        for field in fields:
                            if field in doc:
                                projected_doc[field] = doc[field]
                    self._print_doc(projected_doc, io_output=io_output)
        print("filtering succeeded", file=io_output)
        return True
    
    def order(self, table_name: str, field: str, order_method: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        # do external sorting
        temp_sorted_file = self._external_sort(table_name, field, order_method)
        # print the sorted file
        with open(temp_sorted_file, 'r') as f:
            doc = self._next_doc(f)
            while doc is not None:
                self._print_doc(doc, io_output=io_output)
                doc = self._next_doc(f)
        clear_temp_files()
        print("order succeeded", file=io_output)
        return True
    
    def aggregate(self, table_name: str, aggregate_method: str, aggregate_field: str, group_field: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        
        # do external sorting
        temp_sorted_file = self._external_sort(table_name, group_field, "asc")
        # aggregate
        with open(temp_sorted_file, 'r') as f:
            doc = self._next_doc(f)
            if doc is None:
                print("No data to aggregate!", file=io_output)
                return True
            # initialize the group
            cur_group_result = None
            pre_group_by_field_value = None
            while doc is not None:
                # get the group_by_field value of the current doc
                cur_group_by_field_value = doc[group_field]
                # if the group_by_field value changes, output the aggregate result of the previous group
                if cur_group_by_field_value != pre_group_by_field_value and pre_group_by_field_value is not None:
                    if cur_group_result is not None and aggregate_method == "avg":
                        cur_group_result = round(get_key_val(cur_group_result[0]) / cur_group_result[1], 2)
                    elif cur_group_by_field_value is not None and (aggregate_method == "min" or aggregate_method == "max" or aggregate_method == "sum"):
                        cur_group_result = get_key_val(cur_group_result)
                    if cur_group_result is None:
                        cur_group_result = 0
                    self._print_doc({group_field: pre_group_by_field_value, f"{aggregate_method}({aggregate_field})": cur_group_result}, io_output=io_output)
                    # reset the group result
                    cur_group_result = None
                # if the current row is in the same group as the previous row, update the aggregate result
                if aggregate_field in doc:
                    cur_aggregate_field_value = mix_key(doc[aggregate_field])
                else:
                    cur_aggregate_field_value = mix_key(0)
                if aggregate_method == "sum":
                    if cur_group_result is None:
                        cur_group_result = mix_key(0)
                    cur_group_result = add_key(cur_group_result, cur_aggregate_field_value)
                elif aggregate_method == "avg":
                    if cur_group_result is None:
                        cur_group_result = [mix_key(0), 0]
                    cur_group_result[0] = add_key(cur_group_result[0], cur_aggregate_field_value)
                    cur_group_result[1] += 1
                elif aggregate_method == "count":
                    if cur_group_result is None:
                        cur_group_result = 0
                    cur_group_result += 1
                elif aggregate_method == "max":
                    if cur_group_result is None:
                        cur_group_result = cur_aggregate_field_value
                    else:
                        cur_group_result = max(cur_group_result, cur_aggregate_field_value)
                elif aggregate_method == "min":
                    if cur_group_result is None:
                        cur_group_result = cur_aggregate_field_value
                    else:
                        cur_group_result = min(cur_group_result, cur_aggregate_field_value)
                doc = self._next_doc(f)
                pre_group_by_field_value = cur_group_by_field_value
            # output the aggregate result of the last group
            if doc is None and pre_group_by_field_value is not None:
                if cur_group_result is not None and aggregate_method == "avg":
                    cur_group_result = round(get_key_val(cur_group_result[0]) / cur_group_result[1], 2)
                elif cur_group_by_field_value is not None and (aggregate_method == "min" or aggregate_method == "max" or aggregate_method == "sum"):
                    cur_group_result = get_key_val(cur_group_result)
                if cur_group_result is None:
                    cur_group_result = 0
                self._print_doc({group_field: pre_group_by_field_value, f"{aggregate_method}({aggregate_field})": cur_group_result}, io_output=io_output)
        clear_temp_files()
        print("aggregation succeeded", file=io_output)
        return True
    
    def aggregate_table(self, table_name: str, aggregate_method: str, aggregate_field: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        # directly iterate through all chunks and aggregate
        cur_result = None
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            for doc in docs:
                if aggregate_field in doc:
                    cur_aggregate_field_value = mix_key(doc[aggregate_field])
                else:
                    cur_aggregate_field_value = mix_key(0)
                if aggregate_method == "sum":
                    if cur_result is None:
                        cur_result = mix_key(0)
                    cur_result = add_key(cur_result, cur_aggregate_field_value)
                elif aggregate_method == "avg":
                    if cur_result is None:
                        cur_result = [mix_key(0), 0]
                    cur_result[0] = add_key(cur_result[0], cur_aggregate_field_value)
                    cur_result[1] += 1
                elif aggregate_method == "count":
                    if cur_result is None:
                        cur_result = 0
                    cur_result += 1
                elif aggregate_method == "max":
                    if cur_result is None:
                        cur_result = cur_aggregate_field_value
                    else:
                        cur_result = max(cur_result, cur_aggregate_field_value)
                elif aggregate_method == "min":
                    if cur_result is None:
                        cur_result = cur_aggregate_field_value
                    else:
                        cur_result = min(cur_result, cur_aggregate_field_value)
        if cur_result is not None and aggregate_method == "avg":
            cur_result = round(get_key_val(cur_result[0]) / cur_result[1], 2)
        elif aggregate_method == "sum" or aggregate_method == "max" or aggregate_method == "min":
            cur_result = get_key_val(cur_result)
        if cur_result is None:
            cur_result = 0
        self._print_doc({f"{aggregate_method}({aggregate_field})": cur_result}, io_output=io_output)
        print("aggregation succeeded", file=io_output)
        return True
    
    def group(self, table_name: str, group_field: str, io_output=sys.stdout) -> bool:
        # check if table exists
        if not self._table_exists(table_name):
            print(f"Table {table_name} does not exist!", file=io_output)
            return True
        
        # do external sorting
        temp_sorted_file = self._external_sort(table_name, group_field, "asc")
        # group
        with open(temp_sorted_file, 'r') as f:
            doc = self._next_doc(f)
            if doc is None:
                print("No data to group!", file=io_output)
                return True
            pre_group_by_field_value = None
            while doc is not None:
                # get the group_by_field value of the current doc
                cur_group_by_field_value = doc[group_field]
                # if the group_by_field value changes, output the group result of the previous group
                if cur_group_by_field_value != pre_group_by_field_value and pre_group_by_field_value is not None:
                    self._print_doc({group_field: pre_group_by_field_value}, io_output=io_output)
                doc = self._next_doc(f)
                pre_group_by_field_value = cur_group_by_field_value
            # output the group result of the last group
            if doc is None and pre_group_by_field_value is not None:
                self._print_doc({group_field: pre_group_by_field_value}, io_output=io_output)
        clear_temp_files()
        print("grouping succeeded", file=io_output)
        return True

    def join(self, left: str, right: str, condition: str, io_output=sys.stdout) -> bool:
        # check if tables exist
        if not self._table_exists(left):
            print(f"Table {left} does not exist!", file=io_output)
            return True
        if not self._table_exists(right):
            print(f"Table {right} does not exist!", file=io_output)
            return True
        # extract the fields from the condition
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        if match is None:
            print(f"Invalid condition {condition}!", file=io_output)
            return True
        left_field, op, right_field = match.groups()
        for right_chunk in self._get_table_chunks(right):
            right_docs = self._read_docs_from_file(right_chunk)
            for left_chunk in self._get_table_chunks(left):
                left_docs = self._read_docs_from_file(left_chunk)
                for right_doc in right_docs:
                    for left_doc in left_docs:
                        if not right_field in right_doc:
                            continue
                        if not left_field in left_doc:
                            continue
                        right_field_value = right_doc[right_field]
                        if self._doc_meets_condition(left_doc, f"{left_field}{op}{right_field_value}"):
                            joined_doc = {}
                            for field in left_doc:
                                joined_doc[f"{left}.{field}"] = left_doc[field]
                            for field in right_doc:
                                joined_doc[f"{right}.{field}"] = right_doc[field]
                            self._print_doc(joined_doc, io_output=io_output)
        print("join succeeded", file=io_output)
        return True
        
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For r/w json files 
    # ========================================================

    def _write_doc_to_file(self, doc: dict, file_path: str):
        with open(file_path, 'a') as f:
            f.write(json.dumps(doc) + "\n")

    def _write_docs_to_file(self, docs: list, file_path: str):
        with open(file_path, 'a') as f:
            for doc in docs:
                 f.write(json.dumps(doc) + "\n")

    def _read_docs_from_file(self, file_path: str) -> list:
        docs_data = []
        with open(file_path, 'r') as f:
            docs_data = [json.loads(line.rstrip("\n")) for line in f.readlines()]
        return docs_data
    
    def _next_doc(self, opened_file) -> dict or None:
        line = next(opened_file, None)
        if line is None:
            return None
        return json.loads(line.rstrip("\n"))
    
    def _clear_file(self, file_path: str) -> None:
        with open(file_path, 'r+') as f:
            f.truncate(0)

    def _get_typed_value(self, val: str) -> int or float or str:
        if val.isdigit():
            return int(val)
        elif val.replace(".", "", 1).isdigit():
            return float(val)
        else:
            return val
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                  For chunk management
    # ========================================================

    def _get_table_path(self, table_name: str) -> str:
        return f"{BASE_DIR}/Storage/NoSQL/{table_name}"
    
    def _table_exists(self, table_name: str) -> bool:
        table_storage_path = f"{BASE_DIR}/Storage/NoSQL/{table_name}"
        if not os.path.exists(table_storage_path):
            return False
        return True

    def _get_chunk_number(self, chunk_path: str) -> int:
        return int(chunk_path.split("/")[-1].split(".")[0].split("_")[-1])
    
    def _get_chunk_path(self, table_name: str, chunk_num: int) -> str:
        return f"{self._get_table_path(table_name)}/chunk_{chunk_num}"
    
    def _get_chunk_size(self, chunk_path: str) -> int:
        with open(chunk_path, 'r') as f:
            return len(f.readlines())
        
    def _get_table_chunks(self, table_name: str) -> list:
        table_storage_path = self._get_table_path(table_name)
        chunks = []
        for file in os.listdir(table_storage_path):
            chunks.append(f"{table_storage_path}/{file}")
        return chunks
        
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For doc operations
    # ========================================================

    def _csv_row_to_doc(self, row, schema) -> dict:
        doc = {}
        for i in range(len(schema)):
            doc[schema[i]] = self._get_typed_value(row[i])
        return doc
    
    def _insert_doc(self, table_name: str, doc: dict) -> None:
         # iterate through all chunks in this directory and find the chunk_num with max num, -1 if no chunks
        max_chunk_num = max([self._get_chunk_number(chunk) for chunk in self._get_table_chunks(table_name)], default=-1)
        if max_chunk_num == -1:
            # if no chunks, create a new chunk
            chunk_path = self._get_chunk_path(table_name, 0)
            self._write_doc_to_file(doc, chunk_path)
        else:
            # if chunks exist, find the chunk with max num
            chunk_path = self._get_chunk_path(table_name, max_chunk_num)
            # check if chunk is full
            if self._get_chunk_size(chunk_path) < CHUNK_SIZE:
                # if not full, append to the chunk
                self._write_doc_to_file(doc, chunk_path)
            else:
                # if full, create a new chunk
                chunk_path = self._get_chunk_path(table_name, max_chunk_num + 1)
                self._write_doc_to_file(doc, chunk_path)

    def _doc_meets_condition(self, doc: dict, condition: str) -> bool:
        match = re.match(r"(.*?)\s*(!=|=|>=|<=|>|<)\s*(.*)", condition)
        field, op, value = match.groups()
        value = self._get_typed_value(value)
        # check if doc has the field
        if field not in doc:
            return False
        # get the doc field value
        doc_field_value = doc[field]
        # check if doc field value is of the same type as value
        if type(doc_field_value) != type(value):
            if type(doc_field_value) == str or type(value) == str:
                return False
            elif type(doc_field_value) == float or type(value) == float:
                # if one of them is float, convert both to float
                value = float(value)
                doc_field_value = float(doc_field_value)
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
        # check if doc field value meets the condition
        return op_func(doc_field_value, value)
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For temp files
    # ========================================================
    
    def _temp_file_name(self, chunk_num: int, pass_num: int) -> str:
        return f"{TEMP_DIR}/chunk_{chunk_num}_pass_{pass_num}"
    
    def _get_chunk_number_from_temp_file(self, temp_file_name: str) -> int:
        # example: Temp/chunk_0_pass_0
        return int(temp_file_name.split("/")[-1].split(".")[0].split("_")[1])
    
    def _get_pass_number_from_temp_file(self, temp_file_name: str) -> int:
        # example: Temp/chunk_0_pass_0
        return int(temp_file_name.split("/")[-1].split(".")[0].split("_")[3])
    
    def _get_temp_chunks(self) -> list:
        temp_chunks = []
        for file in os.listdir(TEMP_DIR):
            if file.endswith(".gitkeep") or file.endswith(".keep"):
                continue
            temp_chunks.append(f"{TEMP_DIR}/{file}")
        return temp_chunks
    
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For external sort
    # ========================================================

    def _external_sort(self, table_name: str, field: str, order_method: str) -> str:
        # sorting phase
        for chunk in self._get_table_chunks(table_name):
            docs = self._read_docs_from_file(chunk)
            # ignore docs that don't have the field
            docs = filter(lambda doc: field in doc, docs)
            sorted_docs = sorted(docs, key=lambda doc: mix_key(doc[field]), reverse=order_method == "desc")
            # write the sorted docs to the temp directory
            chunk_num = self._get_chunk_number(chunk)
            self._write_docs_to_file(sorted_docs, self._temp_file_name(chunk_num, 0))
        # merge the sorted chunks
        return self._merge_sorted_chunks(field, order_method, 0)

    def _merge_sorted_chunks(self, field, order_method, pass_num) -> str:
        # find the max chunk number under the temp directory and skip the chunks not in the current pass
        # return -1 if no chunks
        max_chunk_num = max([self._get_chunk_number_from_temp_file(chunk) for chunk in self._get_temp_chunks() if self._get_pass_number_from_temp_file(chunk) == pass_num], default=-1)
        if max_chunk_num == -1:
            raise Exception("No data in the temp directory!")
        
        if max_chunk_num == 0:
            # return the final run
            return self._temp_file_name(0, pass_num)
        
        # Use CHUNK_SIZE-way merging
        next_chunk_num = 0 # the chunk number of the merged file in the next pass
        start_chunk_num = 0 # the starting chunk of current merge group
        end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1) # the ending chunk of current merge group

        while start_chunk_num <= max_chunk_num:
            merged_file_path = self._temp_file_name(next_chunk_num, pass_num + 1)
            opened_files = {}
            loaded_docs = PriorityQueue() # pq of DocElement
            for chunk_num in range(start_chunk_num, end_chunk_num):
                opened_files[chunk_num] = open(self._temp_file_name(chunk_num, pass_num), 'r')
            # load the first doc from each file
            for chunk_num in range(start_chunk_num, end_chunk_num):
                doc = self._next_doc(opened_files[chunk_num])
                if doc is not None:
                    loaded_docs.put(DocElement(chunk_num, doc, field, order_method))
            # output until the pq is empty
            while not loaded_docs.empty():
                doc_element = loaded_docs.get()
                # write the doc to the merged file
                self._write_doc_to_file(doc_element.doc, merged_file_path)
                # load the next doc from the same file that the doc was read from
                next_doc = self._next_doc(opened_files[doc_element.get_chunk_num()])
                if next_doc is not None:
                    loaded_docs.put(DocElement(doc_element.get_chunk_num(), next_doc, field, order_method))
            # close all opened files
            for _, opened_file in opened_files.items():
                opened_file.close()
            opened_files.clear()
            # update the start/end_chunk_num and next_chunk_num and proceed to the next merge group
            start_chunk_num = end_chunk_num
            end_chunk_num = min(start_chunk_num + CHUNK_SIZE, max_chunk_num + 1)
            next_chunk_num += 1
        # proceed to the next pass
        return self._merge_sorted_chunks(field, order_method, pass_num + 1)
                
    # ========================================================
    #                  ***** Helpers *****
    #
    #                   For printing docs
    # ========================================================
    
    def _print_doc(self, doc: dict, io_output=sys.stdout) -> None:
        print(json.dumps(doc, indent=4), file=io_output)
            