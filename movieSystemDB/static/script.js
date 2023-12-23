async function post(url, data){
    const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-type': 'application/json'
        },
        body: JSON.stringify(data)
    });
    const resData = await response.text()
    return resData;
}

function display(result) {
    const resultDisplay = document.getElementById("resultDisplay");
    resultDisplay.innerHTML = result;
}

function getEngineType() {
    const isRelational = document.getElementById("relationalCheck").checked;
    if (isRelational) {
        return "relational";
    } else {
        return "nosql";
    }
}

async function load() {
    const engine = getEngineType();
    const datasetToLoad = document.getElementById("datasetToLoad").files[0]
    if (datasetToLoad) {
        const formData = new FormData();
        formData.append("file", datasetToLoad);
        formData.append("engine", engine);
        try {
            res = await fetch("/load", {
                method: 'POST',
                body: formData
            });
            const resData = await res.text();
            display(resData);
        } catch(err) {
            console.log(err)
        }
    }
}

async function projection() {
    const engine = getEngineType();
    const projectionTableName = document.getElementById("projectionTableName").value;
    const projectionFields = document.getElementById("projectionFields").value;
    data = {
        table_name: projectionTableName,
        fields: projectionFields,
        engine: engine
    }
    try {
        res = await post("/projection", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function filtering() {
    const engine = getEngineType();
    const filteringTableName = document.getElementById("filteringTableName").value;
    const filteringFields = document.getElementById("filteringFields").value;
    const filteringCondition = document.getElementById("filteringCondition").value;
    data = {
        table_name: filteringTableName,
        fields: filteringFields,
        condition: filteringCondition,
        engine: engine
    }
    try {
        res = await post("/filtering", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function updating() {
    const engine = getEngineType();
    const updatingTableName = document.getElementById("updatingTableName").value;
    const updatingCondition = document.getElementById("updatingCondition").value;
    const updatingData = document.getElementById("updatingData").value;
    data = {
        table_name: updatingTableName,
        condition: updatingCondition,
        data: updatingData,
        engine: engine
    }
    try {
        res = await post("/updating", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function deletion() {
    const engine = getEngineType();
    const deletionTableName = document.getElementById("deletionTableName").value;
    const deletionCondition = document.getElementById("deletionCondition").value;
    data = {
        table_name: deletionTableName,
        condition: deletionCondition,
        engine: engine
    }
    try {
        res = await post("/deletion", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function insertion() {
    const engine = getEngineType();
    const insertionTableName = document.getElementById("insertionTableName").value;
    const insertionData = document.getElementById("insertionData").value;
    data = {
        table_name: insertionTableName,
        data: insertionData,
        engine: engine
    }
    try {
        res = await post("/insertion", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function sorting() {
    const engine = getEngineType();
    const sortingTableName = document.getElementById("sortingTableName").value;
    const sortingField = document.getElementById("sortingField").value;
    const sortingASC = document.getElementById("sortingASC").checked;
    data = {
        table_name: sortingTableName,
        field: sortingField,
        method: sortingASC ? "asc" : "desc",
        engine: engine
    }
    try {
        res = await post("/sorting", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function join() {
    const engine = getEngineType();
    const joinLeftTable = document.getElementById("joinLeftTable").value;
    const joinRightTable = document.getElementById("joinRightTable").value;
    const joinCondition = document.getElementById("joinCondition").value;
    data = {
        left_table: joinLeftTable,
        right_table: joinRightTable,
        condition: joinCondition,
        engine: engine
    }
    try {
        res = await post("/join", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function aggregate() {
    const engine = getEngineType();
    const aggregateTableName = document.getElementById("aggregateTableName").value;
    const aggregateToFind = document.getElementById("aggregateToFind").value;
    const aggregateGroupBy = document.getElementById("aggregateGroupBy").value;
    data = {
        table_name: aggregateTableName,
        to_find: aggregateToFind,
        group_by: aggregateGroupBy,
        engine: engine
    }
    try {
        res = await post("/aggregate", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}