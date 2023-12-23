from flask import Flask, jsonify, render_template, request, send_from_directory
from Engine.nosql import NoSQL
from Engine.relational import Relational
import re

from config import BASE_DIR

app = Flask(__name__)
app.config["RESULT_DIR"] = f"{BASE_DIR}/Results"
app.config["RELATIONAL_ENGINE"] = Relational()
app.config["NOSQL_ENGINE"] = NoSQL()

@app.route('/')
def index():
    return send_from_directory("static", "index.html")

@app.route('/load', methods=['POST'])
def load():
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    engine = request.form['engine']
    try:
        datasetToLoad = request.files['file']
        if datasetToLoad:
            datasetToLoad.save(f'ToBeLoaded/{datasetToLoad.filename}')
        if engine == 'relational':
            ok = app.config["RELATIONAL_ENGINE"].load_data(datasetToLoad.filename, io_output)
        else:
            ok = app.config["NOSQL_ENGINE"].load_data(datasetToLoad.filename, io_output)
        if not ok:
            return "Error occurred"
        # close output file
        io_output.close()
        return send_from_directory(app.config["RESULT_DIR"], "result.txt")
    except Exception as e:
        return jsonify({'error': f'Error occurred: {str(e)}'}), 500

@app.route('/projection', methods=['POST'])
def show_data():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    fields = data.get('fields').split(',')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].projection(table_name, fields, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].projection(table_name, fields, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/filtering', methods=['POST'])
def filtering():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    fields = data.get('fields').split(',')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].filtering(table_name, fields, condition, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].filtering(table_name, fields, condition, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/updating', methods=['POST'])
def updating():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    data_val = data.get('data').split(',')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].update_data(table_name, condition, data_val, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].update_data(table_name, condition, data_val, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/deletion', methods=['POST'])
def deletion():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].delete_data(table_name, condition, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].delete_data(table_name, condition, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/insertion', methods=['POST'])
def insertion():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    data_val = data.get('data').split(',')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].insert_data(table_name, data_val, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].insert_data(table_name, data_val, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/sorting', methods=['POST'])
def sorting():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    field = data.get('field')
    method = data.get('method')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].order(table_name, field, method, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].order(table_name, field, method, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/join', methods=['POST'])
def join():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    left_table = data.get('left_table')
    right_table = data.get('right_table')
    condition = data.get('condition')
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].join(left_table, right_table, condition, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].join(left_table, right_table, condition, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

@app.route('/aggregate', methods=['POST'])
def aggregate():
    # Get data from request
    data = request.get_json()
    engine = data.get('engine')
    table_name = data.get('table_name')
    to_find = data.get('to_find')
    group_by = data.get('group_by')
    if to_find == '':
        group(engine, table_name, group_by)
    elif group_by == '':
        aggregation_method = re.match(r'(.*?)\((.*?)\)', to_find).group(1)
        aggregation_field = re.match(r'(.*?)\((.*?)\)', to_find).group(2)
        aggregate_table(engine, table_name, aggregation_method, aggregation_field)
    else:
        aggregation_method = re.match(r'(.*?)\((.*?)\)', to_find).group(1)
        aggregation_field = re.match(r'(.*?)\((.*?)\)', to_find).group(2)
        aggregate_group(engine, table_name, aggregation_method, aggregation_field, group_by)
    return send_from_directory(app.config["RESULT_DIR"], "result.txt")

def aggregate_group(engine, table_name, aggregation_method, aggregation_field, group_field):
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].aggregate(table_name, aggregation_method, aggregation_field, group_field, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].aggregate(table_name, aggregation_method, aggregation_field, group_field, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"

def aggregate_table(engine, table_name, aggregation_method, aggregation_field):
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].aggregate_table(table_name, aggregation_method, aggregation_field, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].aggregate_table(table_name, aggregation_method, aggregation_field, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"

def group(engine, table_name, group_field):
    # open output file
    io_output = open(f"{app.config['RESULT_DIR']}/result.txt", "w")
    # call the specified engine
    if engine == 'relational':
        ok = app.config["RELATIONAL_ENGINE"].group(table_name, group_field, io_output)
    else:
        ok = app.config["NOSQL_ENGINE"].group(table_name, group_field, io_output)
    # close output file
    io_output.close()
    if not ok:
        return "Error occurred"


if __name__ == "__main__":
	app.run()