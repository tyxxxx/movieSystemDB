from abc import abstractmethod
import re


class BaseEngine():
    command_dict = {
        "list_all_tables": r'show tables;',
        "create_table": r'create table (.*?);',
        "drop_table": r'drop table (.*?);',
        "insert_data": r'insert into (.*?) with data (.*?);',
        "delete_data": r'delete from (.*?) where (.*?);',
        "update_data": r'update in (.*?) where (.*?) and set (.*?);',
        "projection": r'show field (.*?) from (.*?);',
        "filtering": r'show data (.*?) from (.*?) where (.*?);',
        "join": r'join (.*?) and (.*?) on (.*?);',
        "aggregate": r'find (.*?) in (.*?) group by (.*?);',
        "order": r'sort data in (.*?) by (.*?) (.*?);',
        "exit": r'exit',
        "load_data": r'load data from (.*?);',
        "aggregate_table": r'find (.*?) in (.*?);',
        "group": r"group (.*?) by (.*?);"
    }

    def parse_and_execute(self, input_str):
        if not input_str.endswith(';'):
            print("all queries must end with a semicolon ';'")
            return True
        elif re.match(self.command_dict['exit'], input_str):
            return False
        elif re.match(self.command_dict['load_data'], input_str):
            # load data
            # example: load data from xxx.csv
            file_name = re.match(self.command_dict['load_data'], input_str).group(1)
            print(file_name)
            return self.load_data(file_name)
        elif re.match(self.command_dict['list_all_tables'], input_str):
            # show all tables
            # example: show tables
            return self.show_tables()
        elif re.match(self.command_dict['create_table'], input_str):
            # create table
            # example: create table table_name(field1,field2,field3)
            kwargs = re.match(self.command_dict['create_table'], input_str)
            match = re.match(r'(.*?)\((.*?)\)', kwargs.group(1))
            if match is None:
                print("invalid query: check the table specification")
                return True
            table_name = match.group(1)
            fields = match.group(2).split(',')
            return self.create_table(table_name, fields)
        elif re.match(self.command_dict['drop_table'], input_str):
            # drop table
            # example: drop table table_name
            table_name = re.match(self.command_dict['drop_table'], input_str).group(1)
            return self.drop_table(table_name)
        elif re.match(self.command_dict['insert_data'], input_str):
            # insert data
            # example: insert into table_name with data id=4,address=east42
            kwargs = re.match(self.command_dict['insert_data'], input_str)
            table_name = kwargs.group(1)
            data = kwargs.group(2).split(',')
            return self.insert_data(table_name, data)
        elif re.match(self.command_dict['delete_data'], input_str):
            # delete data
            # example: delete from table_name where id=4
            kwargs = re.match(self.command_dict['delete_data'], input_str)
            table_name = kwargs.group(1)
            condition = kwargs.group(2)
            return self.delete_data(table_name, condition)
        elif re.match(self.command_dict['update_data'], input_str):
            # update data
            # example: update in table_name where id=4 and set address=east42,id=5
            kwargs = re.match(self.command_dict['update_data'], input_str)
            table_name = kwargs.group(1)
            condition = kwargs.group(2)
            data = kwargs.group(3).split(',')
            return self.update_data(table_name, condition, data)
        elif re.match(self.command_dict['projection'], input_str):
            # projection
            # example: show column id,name from table_name
            kwargs = re.match(self.command_dict['projection'], input_str)
            if kwargs.group(1) == '*':
                fields = ['*']
            else:
                fields = kwargs.group(1).split(',')
            table_name = kwargs.group(2)
            return self.projection(table_name, fields)
        elif re.match(self.command_dict['filtering'], input_str):
            # filtering
            # example: show data id,name from table_name where id=4
            kwargs = re.match(self.command_dict['filtering'], input_str)
            if kwargs.group(1) == '*':
                fields = ['*']
            else:
                fields = kwargs.group(1).split(',')
            table_name = kwargs.group(2)
            condition = kwargs.group(3)
            return self.filtering(table_name, fields, condition)
        elif re.match(self.command_dict['order'], input_str):
            # order
            # example: sort data in table_name by id desc
            kwargs = re.match(self.command_dict['order'], input_str)
            table_name = kwargs.group(1)
            field = kwargs.group(2)
            order_method = kwargs.group(3)
            # check if order_method is valid
            if order_method not in ['asc', 'desc']:
                print("order method must be asc or desc")
                return True
            return self.order(table_name, field, order_method)
        elif re.match(self.command_dict['join'], input_str):
            # join
            # example: join table1 and table2 on table1.id=table2.id
            kwargs = re.match(self.command_dict['join'], input_str)
            table1 = kwargs.group(1)
            table2 = kwargs.group(2)
            condition = kwargs.group(3)
            return self.join(table1, table2, condition)
        elif re.match(self.command_dict['aggregate'], input_str):
            # aggregate
            # example: find max(salary) from table_name group by age;
            kwargs = re.match(self.command_dict['aggregate'], input_str)
            agg = kwargs.group(1)
            aggregation_method = re.match(r'(.*?)\((.*?)\)', agg).group(1)
            aggregation_field = re.match(r'(.*?)\((.*?)\)', agg).group(2)
            if aggregation_method is None or aggregation_field is None:
                print("invalid query: check the format of the aggregation field")
                return True
            # check if aggregation method is valid
            if aggregation_method not in ['max', 'min', 'sum', 'avg', 'count']:
                print("aggregation method must be max, min, sum, avg or count")
                return True
            table_name = kwargs.group(2)
            group_field = kwargs.group(3)
            return self.aggregate(table_name, aggregation_method, aggregation_field, group_field)
        elif re.match(self.command_dict['aggregate_table'], input_str):
            # aggregate table
            # example: find max(salary) from table_name
            kwargs = re.match(self.command_dict['aggregate_table'], input_str)
            agg = kwargs.group(1)
            aggregation_method = re.match(r'(.*?)\((.*?)\)', agg).group(1)
            aggregation_field = re.match(r'(.*?)\((.*?)\)', agg).group(2)
            if aggregation_method is None or aggregation_field is None:
                print("invalid query: check the format of the aggregation field")
                return True
            # check if aggregation method is valid
            if aggregation_method not in ['max', 'min', 'sum', 'avg', 'count']:
                print("aggregation method must be max, min, sum or avg")
                return True
            table_name = kwargs.group(2)
            return self.aggregate_table(table_name, aggregation_method, aggregation_field)
        elif re.match(self.command_dict['group'], input_str):
            # group
            # example: group table_name by age;
            kwargs = re.match(self.command_dict['group'], input_str)
            table_name = kwargs.group(1)
            group_field = kwargs.group(2)
            return self.group(table_name, group_field)
        else:
            print("invalid query")
            return True



    @abstractmethod
    def show_tables(self, output) -> bool:
        pass

    @abstractmethod
    def create_table(self, table_name: str, fields: list, output) -> bool:
        pass

    @abstractmethod
    def drop_table(self, table_name: str, output) -> bool:
        pass

    @abstractmethod
    def insert_data(self, table_name: str, data: list, output) -> bool:
        pass

    @abstractmethod
    def delete_data(self, table_name: str, condition: str, output) -> bool:
        pass

    @abstractmethod
    def update_data(self, table_name: str, condition: str, data: list, output) -> bool:
        pass

    @abstractmethod
    def projection(self, table_name: str, fields: list, output) -> bool:
        pass

    @abstractmethod
    def filtering(self, table_name: str, fields: list, condition: str, output) -> bool:
        pass

    @abstractmethod
    def join(self, left: str, right: str, condition: str, output) -> bool:
        pass

    @abstractmethod
    def group(self, table_name: str, group_field: str, output) -> bool:
        pass

    @abstractmethod
    def aggregate(self, table_name: str, aggregation_method: str, aggregation_field: str, group_field: str, output) -> bool:
        pass

    @abstractmethod
    def aggregate_table(self, table_name: str, aggregation_method: str, aggregation_field: str, output) -> bool:
        pass

    @abstractmethod
    def order(self, table_name: str, field: str, order_method: str, output) -> bool:
        pass

    @abstractmethod
    def load_data(self, file_name: str, output) -> bool:
        pass

