document.getElementById('queryType').addEventListener('change', function () {
    const queryType = this.value;
    const dynamicForm = document.getElementById('dynamicForm');
    const customQuery = document.getElementById('customQuery');
    if (!dynamicForm) return;
    dynamicForm.innerHTML = '';
    customQuery.style.display = 'none';

    if (queryType === 'select_table' || queryType === 'insert' || queryType === 'update' || queryType === 'delete') {
        dynamicForm.innerHTML = `
            <label for="dbSelect">Database:</label>
            <select id="dbSelect" onchange="loadTables()">
                <option value="">Select Database</option>
            </select><br>
            <label for="tableSelect">Table:</label>
            <select id="tableSelect" onchange="${queryType === 'update' || queryType === 'delete' ? 'loadRows()' : 'loadSchema()'}">
                <option value="">Select Table</option>
            </select><br>
        `;
        if (queryType === 'update' || queryType === 'delete') {
            dynamicForm.innerHTML += `
                <label for="rowSelect">Row ID:</label>
                <select id="rowSelect" onchange="${queryType === 'update' ? 'fetchRowData()' : ''}">
                    <option value="">Select Row</option>
                </select><br>
            `;
        }
        loadDatabases();
    } else if (queryType === 'create_db') {
        dynamicForm.innerHTML = `
            <label for="dbName">Database Name:</label>
            <input type="text" id="dbName"><br>
        `;
    } else if (queryType === 'drop_db') {
        dynamicForm.innerHTML = `
            <label for="dbSelect">Select Database to Drop:</label>
            <select id="dbSelect">
                <option value="">Select Database</option>
            </select><br>
        `;
        loadDatabases();
    } else if (queryType === 'create_table') {
        dynamicForm.innerHTML = `
            <label for="dbSelect">Select Database:</label>
            <select id="dbSelect">
                <option value="">Select Database</option>
            </select><br>
            <label for="tableName">Table Name:</label>
            <input type="text" id="tableName"><br>
            <button type="button" onclick="addAttribute()">Add Attribute</button>
            <div id="attributes"></div>
        `;
        loadDatabases();
    } else if (queryType === 'drop_table') {
        dynamicForm.innerHTML = `
            <label for="dbSelect">Select Database:</label>
            <select id="dbSelect" onchange="loadTables()">
                <option value="">Select Database</option>
            </select><br>
            <label for="tableSelect">Select Table to Drop:</label>
            <select id="tableSelect">
                <option value="">Select Table</option>
            </select><br>
        `;
        loadDatabases();
    } else if (queryType === 'mysql_query') {
        customQuery.style.display = 'block';
        dynamicForm.innerHTML = `
            <label for="dbSelect">Database:</label>
            <select id="dbSelect">
                <option value="">Select Database</option>
            </select><br>
        `;
        loadDatabases();
    }
});

function loadDatabases() {
    const dbSelect = document.getElementById('dbSelect');
    if (!dbSelect) return;
    fetch('/databases')
        .then(response => response.json())
        .then(data => {
            dbSelect.innerHTML = '<option value="">Select Database</option>';
            if (data.databases) {
                data.databases.forEach(db => {
                    const option = document.createElement('option');
                    option.value = db;
                    option.textContent = db;
                    dbSelect.appendChild(option);
                });
            }
        })
        .catch(error => {
            console.error('Error fetching databases:', error);
            const result = document.getElementById('result');
            if (result) result.innerHTML = 'Error fetching databases: ' + error;
        });
}

function loadTables() {
    const dbSelect = document.getElementById('dbSelect');
    const tableSelect = document.getElementById('tableSelect');
    if (!dbSelect || !tableSelect) return;
    const dbName = dbSelect.value;
    if (!dbName) {
        tableSelect.innerHTML = '<option value="">Select Table</option>';
        return;
    }

    fetch(`/tables?db=${dbName}`)
        .then(response => response.json())
        .then(data => {
            tableSelect.innerHTML = '<option value="">Select Table</option>';
            if (data.tables) {
                data.tables.forEach(table => {
                    const option = document.createElement('option');
                    option.value = table;
                    option.textContent = table;
                    tableSelect.appendChild(option);
                });
            }
        })
        .catch(error => {
            console.error('Error fetching tables:', error);
            const result = document.getElementById('result');
            if (result) result.innerHTML = 'Error fetching tables: ' + error;
        });
}

function loadSchema() {
    const dbSelect = document.getElementById('dbSelect');
    const tableSelect = document.getElementById('tableSelect');
    if (!dbSelect || !tableSelect) return;
    const dbName = dbSelect.value;
    const tableName = tableSelect.value;
    if (!dbName || !tableName) return;

    fetch(`/schema?db=${dbName}&table=${tableName}`)
        .then(response => response.json())
        .then(data => {
            const dynamicForm = document.getElementById('dynamicForm');
            if (!dynamicForm) return;
            const queryType = document.getElementById('queryType').value;

            // Keep the existing dbSelect and tableSelect, and append new fields
            let existingContent = `
                <label for="dbSelect">Database:</label>
                <select id="dbSelect" onchange="loadTables()">
                    <option value="${dbName}">${dbName}</option>
                </select><br>
                <label for="tableSelect">Table:</label>
                <select id="tableSelect" onchange="${queryType === 'update' || queryType === 'delete' ? 'loadRows()' : 'loadSchema()'}">
                    <option value="${tableName}">${tableName}</option>
                </select><br>
            `;

            if (queryType === 'insert') {
                let inputs = '';
                data.columns.forEach(column => {
                    inputs += `
                        <label for="${column.name}">${column.name} (${column.type}):</label>
                        <input type="text" id="${column.name}" name="${column.name}"><br>
                    `;
                });
                dynamicForm.innerHTML = existingContent + inputs;
            }
        })
        .catch(error => {
            console.error('Error fetching schema:', error);
            const result = document.getElementById('result');
            if (result) result.innerHTML = 'Error fetching schema: ' + error;
        });
}

function loadRows() {
    const dbSelect = document.getElementById('dbSelect');
    const tableSelect = document.getElementById('tableSelect');
    const rowSelect = document.getElementById('rowSelect');
    if (!dbSelect || !tableSelect || !rowSelect) return;
    const dbName = dbSelect.value;
    const tableName = tableSelect.value;
    if (!dbName || !tableName) return;

    fetch(`/rows?db=${dbName}&table=${tableName}`)
        .then(response => response.json())
        .then(data => {
            const dynamicForm = document.getElementById('dynamicForm');
            if (!dynamicForm) return;
            const queryType = document.getElementById('queryType').value;

            // Keep the existing dbSelect and tableSelect
            let existingContent = `
                <label for="dbSelect">Database:</label>
                <select id="dbSelect" onchange="loadTables()">
                    <option value="${dbName}">${dbName}</option>
                </select><br>
                <label for="tableSelect">Table:</label>
                <select id="tableSelect" onchange="${queryType === 'update' || queryType === 'delete' ? 'loadRows()' : 'loadSchema()'}">
                    <option value="${tableName}">${tableName}</option>
                </select><br>
                <label for="rowSelect">Row ID:</label>
                <select id="rowSelect" onchange="${queryType === 'update' ? 'fetchRowData()' : ''}">
                    <option value="">Select Row</option>
            `;

            if (data.ids) {
                data.ids.forEach(id => {
                    existingContent += `<option value="${id}">${id}</option>`;
                });
            }
            existingContent += `</select><br>`;
            dynamicForm.innerHTML = existingContent;
        })
        .catch(error => {
            console.error('Error fetching rows:', error);
            const result = document.getElementById('result');
            if (result) result.innerHTML = 'Error fetching rows: ' + error;
        });
}

function fetchRowData() {
    const dbSelect = document.getElementById('dbSelect');
    const tableSelect = document.getElementById('tableSelect');
    const rowSelect = document.getElementById('rowSelect');
    if (!dbSelect || !tableSelect || !rowSelect) return;
    const dbName = dbSelect.value;
    const tableName = tableSelect.value;
    const rowId = rowSelect.value;
    if (!dbName || !tableName || !rowId) return;

    fetch(`/row?db=${dbName}&table=${tableName}&id=${rowId}`)
        .then(response => response.json())
        .then(data => {
            const dynamicForm = document.getElementById('dynamicForm');
            if (!dynamicForm) return;
            let html = `
                <label for="dbSelect">Database:</label>
                <select id="dbSelect" onchange="loadTables()">
                    <option value="${dbName}">${dbName}</option>
                </select><br>
                <label for="tableSelect">Table:</label>
                <select id="tableSelect" onchange="loadRows()">
                    <option value="${tableName}">${tableName}</option>
                </select><br>
                <label for="rowSelect">Row ID:</label>
                <select id="rowSelect" onchange="fetchRowData()">
                    <option value="${rowId}">${rowId}</option>
                </select><br>
                <input type="hidden" id="rowId" value="${rowId}">
            `;
            data.columns.forEach(column => {
                let displayValue = column.value;
                // If the column type contains 'binary' or 'blob', decode base64
                if (column.type && /binary|blob/i.test(column.type)) {
                    try {
                        displayValue = atob(column.value);
                    } catch (e) {
                        displayValue = column.value;
                    }
                }
                html += `
                    <label for="${column.name}">${column.name} (${column.type}):</label>
                    <input type="text" id="${column.name}" name="${column.name}" value="${displayValue}"><br>
                `;
            });
            dynamicForm.innerHTML = html;
        })
        .catch(error => {
            console.error('Error fetching row data:', error);
            const result = document.getElementById('result');
            if (result) result.innerHTML = 'Error fetching row data: ' + error;
        });
}

function addAttribute() {
    const attributesDiv = document.getElementById('attributes');
    const attributeDiv = document.createElement('div');
    attributeDiv.innerHTML = `
        <label for="attrName">Attribute Name:</label>
        <input type="text" class="attrName"><br>
        <label for="attrType">Data Type:</label>
        <select class="attrType">
            <option value="INT">INT</option>
            <option value="VARCHAR(255)">VARCHAR(255)</option>
            <option value="TEXT">TEXT</option>
            <option value="DATE">DATE</option>
        </select><br>
    `;
    attributesDiv.appendChild(attributeDiv);
}

function executeQuery() {
    const queryType = document.getElementById('queryType')?.value;
    const dbSelect = document.getElementById('dbSelect');
    const dbName = dbSelect ? dbSelect.value : '';
    const userType = document.getElementById('userType')?.value;
    const resultDiv = document.getElementById('result');
    if (!resultDiv) return;

    if (queryType === 'select_table') {
        const tableSelect = document.getElementById('tableSelect');
        const tableName = tableSelect ? tableSelect.value : '';
        if (!dbName || !tableName) {
            resultDiv.innerHTML = 'Database and table selection are required';
            return;
        }
        // Fetch schema first
        fetch(`/schema?db=${dbName}&table=${tableName}`)
            .then(schemaResp => schemaResp.json())
            .then(schemaData => {
                const columnTypes = {};
                if (schemaData.columns) {
                    schemaData.columns.forEach(col => {
                        columnTypes[col.name] = col.type;
                    });
                }
                // Now fetch the data
                fetch('/query', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: `userType=${userType}&dbName=${dbName}&query=SELECT * FROM ${tableName}`
                })
                    .then(response => response.json())
                    .then(data => {
                        if (data.error) {
                            resultDiv.innerHTML = data.error;
                        } else {
                            let html = '<h3>Results:</h3><table border="1"><tr>';
                            Object.keys(data.data[0] || {}).forEach(col => html += `<th>${col}</th>`);
                            html += '</tr>';
                            data.data.forEach(row => {
                                html += '<tr>';
                                Object.entries(row).forEach(([col, val]) => {
                                    let displayValue = val;
                                    if (columnTypes[col] && /binary|blob/i.test(columnTypes[col])) {
                                        try {
                                            displayValue = atob(val);
                                        } catch (e) {
                                            displayValue = val;
                                        }
                                    }
                                    html += `<td>${displayValue}</td>`;
                                });
                                html += '</tr>';
                            });
                            html += '</table>';
                            resultDiv.innerHTML = html;
                        }
                    })
                    .catch(error => {
                        console.error('Error executing query:', error);
                        resultDiv.innerHTML = 'Error executing query: ' + error;
                    });
            });
        return;
    } else if (queryType === 'insert') {
        const tableSelect = document.getElementById('tableSelect');
        const tableName = tableSelect ? tableSelect.value : '';
        if (!dbName || !tableName) {
            resultDiv.innerHTML = 'Database and table selection are required';
            return;
        }
        const inputs = document.querySelectorAll('#dynamicForm input');
        let columns = [], values = [];
        inputs.forEach(input => {
            if (input.type !== 'hidden' && input.value) {
                columns.push(input.name);
                values.push(`'${input.value}'`);
            }
        });
        if (columns.length === 0) {
            resultDiv.innerHTML = 'At least one column value is required';
            return;
        }
        const query = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values.join(', ')})`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || `Inserted ${data.rows} row(s)`;
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'update') {
        const tableSelect = document.getElementById('tableSelect');
        const rowIdInput = document.getElementById('rowId');
        const tableName = tableSelect ? tableSelect.value : '';
        const rowId = rowIdInput ? rowIdInput.value : '';
        if (!dbName || !tableName || !rowId) {
            resultDiv.innerHTML = 'Database, table, and ID selection are required';
            return;
        }
        const inputs = document.querySelectorAll('#dynamicForm input:not([type="hidden"])');
        let setClause = [];
        inputs.forEach(input => {
            if (input.name) {
                setClause.push(`${input.name} = '${input.value}'`);
            }
        });
        if (setClause.length === 0) {
            resultDiv.innerHTML = 'At least one column value is required';
            return;
        }
        const query = `UPDATE ${tableName} SET ${setClause.join(', ')} WHERE id = '${rowId}'`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || `Updated ${data.rows} row(s)`;
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'delete') {
        const tableSelect = document.getElementById('tableSelect');
        const rowSelect = document.getElementById('rowSelect');
        const tableName = tableSelect ? tableSelect.value : '';
        const rowId = rowSelect ? rowSelect.value : '';
        if (!dbName || !tableName || !rowId) {
            resultDiv.innerHTML = 'Database, table, and row ID selection are required';
            return;
        }
        const query = `DELETE FROM ${tableName} WHERE id = '${rowId}'`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || `Deleted ${data.rows} row(s)`;
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'create_db') {
        const dbNameInput = document.getElementById('dbName');
        const dbNameValue = dbNameInput ? dbNameInput.value : '';
        if (!dbNameValue) {
            resultDiv.innerHTML = 'Database name is required';
            return;
        }
        const query = `CREATE DATABASE ${dbNameValue}`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbNameValue}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || 'Database created successfully';
                if (!data.error) loadDatabases();
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'drop_db') {
        const dbSelect = document.getElementById('dbSelect');
        const dbNameValue = dbSelect ? dbSelect.value : '';
        if (!dbNameValue) {
            resultDiv.innerHTML = 'Database selection is required';
            return;
        }
        const query = `DROP DATABASE ${dbNameValue}`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbNameValue}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || 'Database dropped successfully';
                if (!data.error) loadDatabases();
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'create_table') {
        const tableNameInput = document.getElementById('tableName');
        const tableName = tableNameInput ? tableNameInput.value : '';
        if (!dbName || !tableName) {
            resultDiv.innerHTML = 'Database and table name are required';
            return;
        }
        const attributes = document.querySelectorAll('#attributes .attrName');
        const attrTypes = document.querySelectorAll('#attributes .attrType');
        if (attributes.length === 0) {
            resultDiv.innerHTML = 'At least one attribute is required';
            return;
        }
        let columns = [];
        for (let i = 0; i < attributes.length; i++) {
            const attrName = attributes[i].value;
            const attrType = attrTypes[i].value;
            if (attrName) {
                columns.push(`${attrName} ${attrType}`);
            }
        }
        const query = `CREATE TABLE ${tableName} (${columns.join(', ')})`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || 'Table created successfully';
                if (!data.error) loadTables();
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'drop_table') {
        const tableSelect = document.getElementById('tableSelect');
        const tableName = tableSelect ? tableSelect.value : '';
        if (!dbName || !tableName) {
            resultDiv.innerHTML = 'Database and table selection are required';
            return;
        }
        const query = `DROP TABLE ${tableName}`;
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                resultDiv.innerHTML = data.error || 'Table dropped successfully';
                if (!data.error) loadTables();
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    } else if (queryType === 'mysql_query') {
        const customQuery = document.getElementById('customQuery');
        const query = customQuery ? customQuery.value : '';
        if (!dbName || !query) {
            resultDiv.innerHTML = 'Database and query are required';
            return;
        }
        // Try to detect table name for SELECT * FROM ...
        let tableNameMatch = query.match(/select\s+\*\s+from\s+([a-zA-Z0-9_]+)/i);
        if (tableNameMatch) {
            let tableName = tableNameMatch[1];
            fetch(`/schema?db=${dbName}&table=${tableName}`)
                .then(schemaResp => schemaResp.json())
                .then(schemaData => {
                    const columnTypes = {};
                    if (schemaData.columns) {
                        schemaData.columns.forEach(col => {
                            columnTypes[col.name] = col.type;
                        });
                    }
                    fetch('/query', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
                    })
                        .then(response => response.json())
                        .then(data => {
                            if (data.error) {
                                resultDiv.innerHTML = data.error;
                            } else if (data.data) {
                                let html = '<h3>Results:</h3><table border="1"><tr>';
                                Object.keys(data.data[0] || {}).forEach(col => html += `<th>${col}</th>`);
                                html += '</tr>';
                                data.data.forEach(row => {
                                    html += '<tr>';
                                    Object.entries(row).forEach(([col, val]) => {
                                        let displayValue = val;
                                        if (columnTypes[col] && /binary|blob/i.test(columnTypes[col])) {
                                            try {
                                                displayValue = atob(val);
                                            } catch (e) {
                                                displayValue = val;
                                            }
                                        }
                                        html += `<td>${displayValue}</td>`;
                                    });
                                    html += '</tr>';
                                });
                                html += '</table>';
                                resultDiv.innerHTML = html;
                            } else {
                                resultDiv.innerHTML = `Query executed successfully, affected ${data.rows} row(s)`;
                            }
                        })
                        .catch(error => {
                            console.error('Error executing query:', error);
                            resultDiv.innerHTML = 'Error executing query: ' + error;
                        });
                });
            return;
        }
        fetch('/query', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `userType=${userType}&dbName=${dbName}&query=${encodeURIComponent(query)}`
        })
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    resultDiv.innerHTML = data.error;
                } else if (data.data) {
                    let html = '<h3>Results:</h3><table border="1"><tr>';
                    Object.keys(data.data[0] || {}).forEach(col => html += `<th>${col}</th>`);
                    html += '</tr>';
                    data.data.forEach(row => {
                        html += '<tr>';
                        Object.values(row).forEach(val => html += `<td>${val}</td>`);
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                } else {
                    resultDiv.innerHTML = `Query executed successfully, affected ${data.rows} row(s)`;
                }
            })
            .catch(error => {
                console.error('Error executing query:', error);
                resultDiv.innerHTML = 'Error executing query: ' + error;
            });
    }
}