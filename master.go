package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

var (
	db       *sql.DB
	mu       sync.Mutex
	shardMap map[string]int
	shardDBs []*sql.DB
	slaves   []net.Conn
)

func main() {
	var err error
	db, err = sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/")
	if err != nil {
		log.Fatal("Error connecting to MySQL:", err)
	}
	defer db.Close()

	// Create shard databases if they don't exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS shard1")
	if err != nil {
		log.Fatal("Error creating shard1 database:", err)
	}
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS shard2")
	if err != nil {
		log.Fatal("Error creating shard2 database:", err)
	}

	shardDBs = make([]*sql.DB, 2)
	shardDBs[0], err = sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/shard1")
	if err != nil {
		log.Fatal("Error connecting to Shard1:", err)
	}
	defer shardDBs[0].Close()
	shardDBs[1], err = sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/shard2")
	if err != nil {
		log.Fatal("Error connecting to Shard2:", err)
	}
	defer shardDBs[1].Close()

	shardMap = make(map[string]int)

	// Start Master TCP Server on port 8083
	listener, err := net.Listen("tcp", ":8083")
	if err != nil {
		log.Fatal("Error starting TCP server:", err)
	}
	defer listener.Close()

	fmt.Println("Master is listening on port 8083")

	// Accept slave connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection:", err)
				continue
			}
			mu.Lock()
			slaves = append(slaves, conn)
			mu.Unlock()
			go handleSlave(conn)
		}
	}()

	// Start Web Frontend on port 8081
	go startFrontend()

	// Keep main goroutine alive
	select {}
}

func handleSlave(conn net.Conn) {
	defer func() {
		mu.Lock()
		for i, c := range slaves {
			if c == conn {
				slaves = append(slaves[:i], slaves[i+1:]...)
				break
			}
		}
		mu.Unlock()
		conn.Close()
	}()

	// Send initial setup commands to slave
	setupCommands := []string{
		"CREATE DATABASE IF NOT EXISTS shard1",
		"CREATE DATABASE IF NOT EXISTS shard2",
	}

	for _, cmd := range setupCommands {
		_, err := conn.Write([]byte("master|" + cmd))
		if err != nil {
			fmt.Println("Error sending setup command to slave:", err)
			return
		}
		// Wait for acknowledgment
		buf := make([]byte, 1024)
		_, err = conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading setup acknowledgment:", err)
			return
		}
	}

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from Slave:", err)
			return
		}
		data := string(buf[:n])
		parts := strings.SplitN(data, "|", 2)
		if len(parts) < 2 {
			conn.Write([]byte("Invalid request"))
			continue
		}

		command := parts[0]
		payload := parts[1]

		if command == "FULL_SYNC" {
			syncData, err := dumpAllDatabases()
			if err != nil {
				conn.Write([]byte("Error syncing databases: " + err.Error()))
				continue
			}
			conn.Write([]byte("FULL_SYNC|" + syncData))
		} else {
			dbName := command
			query := payload

			_, err = db.Exec("USE " + dbName)
			if err != nil {
				conn.Write([]byte("Error selecting database: " + err.Error()))
				continue
			}

			queryType := strings.ToUpper(strings.Split(query, " ")[0])
			if queryType == "CREATE" || queryType == "DROP" {
				conn.Write([]byte("Error: CREATE and DROP are Master-only operations"))
				continue
			}

			// Execute the query on the master itself
			result, err := executeQueryWithSharding(query, dbName)
			if err != nil {
				conn.Write([]byte("Error executing query: " + err.Error()))
				continue
			}

			// Broadcast the query to all other slaves immediately (Synchronous Replication)
			mu.Lock()
			for _, slave := range slaves {
				if slave != conn { // Don't send back to the originating slave
					_, err := slave.Write([]byte(dbName + "|" + query))
					if err != nil {
						fmt.Println("Error sending to Slave:", err)
					}
				}
			}
			mu.Unlock()

			conn.Write([]byte("Query executed: " + result))
		}
	}
}

func executeQueryWithSharding(query, dbName string) (string, error) {
	queryType := strings.ToUpper(strings.Split(query, " ")[0])
	var targetDB *sql.DB = db
	var shardID int

	if queryType == "SELECT" || queryType == "INSERT" || queryType == "UPDATE" || queryType == "DELETE" {
		parts := strings.Fields(query)
		var tableName string
		for i, part := range parts {
			if strings.ToUpper(part) == "FROM" || strings.ToUpper(part) == "INTO" {
				tableName = parts[i+1]
				break
			}
		}

		if tableName != "" {
			var exists bool
			shardID, exists = shardMap[tableName]
			if !exists {
				shardID = len(shardMap) % 2
				shardMap[tableName] = shardID
				fmt.Printf("Assigned %s to Shard %d\n", tableName, shardID)
			}
			if shardID >= 0 && shardID < 2 {
				targetDB = shardDBs[shardID]
				fmt.Printf("Executing %s on Shard %d\n", query, shardID)
				_, err := targetDB.Exec("USE " + dbName)
				if err != nil {
					fmt.Printf("Error selecting database %s on shard %d: %v\n", dbName, shardID, err)
					return "", err
				}
			}
		}
	}

	result, err := targetDB.Exec(query)
	if err != nil {
		fmt.Printf("Error executing query on shard %d: %s\nError: %v\n", shardID, query, err)
		return "", err
	}
	fmt.Printf("Executed query on shard %d: %s\n", shardID, query)
	rowsAffected, _ := result.RowsAffected()
	return fmt.Sprintf("Rows affected: %d", rowsAffected), nil
}

func dumpAllDatabases() (string, error) {
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var dump strings.Builder
	for rows.Next() {
		var dbName string
		rows.Scan(&dbName)
		if dbName == "information_schema" || dbName == "mysql" || dbName == "performance_schema" || dbName == "sys" {
			continue
		}

		_, err := db.Exec("USE " + dbName)
		if err != nil {
			return "", err
		}

		dump.WriteString(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;\n", dbName))
		dump.WriteString(fmt.Sprintf("USE %s;\n", dbName))

		tables, err := db.Query("SHOW TABLES")
		if err != nil {
			return "", err
		}
		defer tables.Close()

		for tables.Next() {
			var tableName string
			tables.Scan(&tableName)

			createRow, err := db.Query("SHOW CREATE TABLE " + tableName)
			if err != nil {
				return "", err
			}
			defer createRow.Close()

			if createRow.Next() {
				var temp string
				var createStmt string
				createRow.Scan(&temp, &createStmt)
				dump.WriteString(createStmt + ";\n")
			}

			dataRows, err := db.Query("SELECT * FROM " + tableName)
			if err != nil {
				return "", err
			}
			defer dataRows.Close()

			columns, err := dataRows.Columns()
			if err != nil {
				return "", err
			}

			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			for dataRows.Next() {
				err = dataRows.Scan(valuePtrs...)
				if err != nil {
					return "", err
				}
				var vals []string
				for _, val := range values {
					switch v := val.(type) {
					case nil:
						vals = append(vals, "NULL")
					case string:
						vals = append(vals, fmt.Sprintf("'%s'", v))
					default:
						vals = append(vals, fmt.Sprintf("%v", v))
					}
				}
				dump.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n", tableName, strings.Join(columns, ","), strings.Join(vals, ",")))
			}
		}
	}

	return dump.String(), nil
}

func startFrontend() {
	r := gin.Default()
	r.LoadHTMLGlob("templates/*.html")
	r.Static("/static", "./static")

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{
			"IsMaster": true,
		})
	})

	r.GET("/databases", func(c *gin.Context) {
		rows, err := db.Query("SHOW DATABASES")
		if err != nil {
			log.Println("Error fetching databases:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()
		var databases []string
		for rows.Next() {
			var dbName string
			rows.Scan(&dbName)
			if dbName != "information_schema" && dbName != "mysql" && dbName != "performance_schema" && dbName != "sys" {
				databases = append(databases, dbName)
			}
		}
		c.JSON(http.StatusOK, gin.H{"databases": databases})
	})

	r.GET("/tables", func(c *gin.Context) {
		dbName := c.Query("db")
		if dbName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Database name is required"})
			return
		}
		_, err := db.Exec("USE " + dbName)
		if err != nil {
			log.Println("Error selecting database:", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Error selecting database: " + err.Error()})
			return
		}
		rows, err := db.Query("SHOW TABLES")
		if err != nil {
			log.Println("Error fetching tables:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()
		var tables []string
		for rows.Next() {
			var tableName string
			rows.Scan(&tableName)
			tables = append(tables, tableName)
		}
		c.JSON(http.StatusOK, gin.H{"tables": tables})
	})

	r.GET("/schema", func(c *gin.Context) {
		dbName := c.Query("db")
		tableName := c.Query("table")
		if dbName == "" || tableName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Database and table names are required"})
			return
		}
		_, err := db.Exec("USE " + dbName)
		if err != nil {
			log.Println("Error selecting database:", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Error selecting database: " + err.Error()})
			return
		}
		rows, err := db.Query("DESCRIBE " + tableName)
		if err != nil {
			log.Println("Error describing table:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		defer rows.Close()
		var columns []map[string]string
		for rows.Next() {
			var columnName, columnType, null, key string
			var defaultVal, extra *string
			err = rows.Scan(&columnName, &columnType, &null, &key, &defaultVal, &extra)
			if err != nil {
				log.Println("Error scanning column info:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning column info: " + err.Error()})
				return
			}
			columns = append(columns, map[string]string{
				"name": columnName,
				"type": columnType,
			})
		}
		c.JSON(http.StatusOK, gin.H{"columns": columns})
	})

	r.GET("/rows", func(c *gin.Context) {
		dbName := c.Query("db")
		tableName := c.Query("table")
		if dbName == "" || tableName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Database and table names are required"})
			return
		}
		_, err := db.Exec("USE " + dbName)
		if err != nil {
			log.Println("Error selecting database:", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Error selecting database: " + err.Error()})
			return
		}
		rows, err := db.Query("SELECT id FROM " + tableName)
		if err != nil {
			log.Println("Error fetching rows:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error fetching rows: " + err.Error()})
			return
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			err := rows.Scan(&id)
			if err != nil {
				log.Println("Error scanning ID:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning ID: " + err.Error()})
				return
			}
			ids = append(ids, id)
		}
		c.JSON(http.StatusOK, gin.H{"ids": ids})
	})

	r.GET("/row", func(c *gin.Context) {
		dbName := c.Query("db")
		tableName := c.Query("table")
		id := c.Query("id")
		if dbName == "" || tableName == "" || id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Database, table, and ID are required"})
			return
		}
		log.Println("Fetching row for db:", dbName, "table:", tableName, "id:", id)

		_, err := db.Exec("USE " + dbName)
		if err != nil {
			log.Println("Error selecting database:", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "Error selecting database: " + err.Error()})
			return
		}

		columnRows, err := db.Query("DESCRIBE " + tableName)
		if err != nil {
			log.Println("Error describing table:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error describing table: " + err.Error()})
			return
		}
		defer columnRows.Close()

		var columnNames []string
		var columnTypes = make(map[string]string)
		for columnRows.Next() {
			var columnName, columnType, null, key string
			var defaultVal, extra *string
			err = columnRows.Scan(&columnName, &columnType, &null, &key, &defaultVal, &extra)
			if err != nil {
				log.Println("Error scanning column info:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning column info: " + err.Error()})
				return
			}
			if columnName != "id" {
				columnNames = append(columnNames, columnName)
				columnTypes[columnName] = columnType
			}
		}

		if len(columnNames) == 0 {
			log.Println("No columns found in table (excluding id)")
			c.JSON(http.StatusBadRequest, gin.H{"error": "No columns found in table (excluding id)"})
			return
		}

		var selectColumns []string
		for _, col := range columnNames {
			if strings.Contains(strings.ToLower(columnTypes[col]), "binary") {
				selectColumns = append(selectColumns, fmt.Sprintf("CONVERT(CAST(%s AS CHAR), CHAR) AS %s", col, col))
			} else {
				selectColumns = append(selectColumns, col)
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", strings.Join(selectColumns, ", "), tableName)

		rows, err := db.Query(query, id)
		if err != nil {
			log.Println("Error fetching row:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error fetching row: " + err.Error()})
			return
		}
		defer rows.Close()

		if !rows.Next() {
			log.Println("No row found with id:", id)
			c.JSON(http.StatusNotFound, gin.H{"error": "No row found with id: " + id})
			return
		}

		values := make([]string, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Println("Error scanning row:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning row: " + err.Error()})
			return
		}

		var columnData []map[string]string
		for i, col := range columnNames {
			columnData = append(columnData, map[string]string{
				"name":  col,
				"type":  columnTypes[col],
				"value": values[i],
			})
		}
		c.JSON(http.StatusOK, gin.H{"columns": columnData})
	})

	r.POST("/query", func(c *gin.Context) {
		userType := c.PostForm("userType")
		dbName := c.PostForm("dbName")
		query := c.PostForm("query")

		queryType := strings.ToUpper(strings.Split(query, " ")[0])
		if queryType != "CREATE" || !strings.Contains(strings.ToUpper(query), "DATABASE") {
			_, err := db.Exec("USE " + dbName)
			if err != nil {
				log.Println("Error selecting database:", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "Error selecting database: " + err.Error()})
				return
			}
		}

		if userType != "master" && (queryType == "CREATE" || queryType == "DROP") {
			c.JSON(http.StatusForbidden, gin.H{"error": "CREATE and DROP are Master-only operations"})
			return
		}

		if queryType == "SELECT" {
			rows, err := db.Query(query)
			if err != nil {
				log.Println("Error executing query:", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "Error executing query: " + err.Error()})
				return
			}
			defer rows.Close()
			columns, _ := rows.Columns()
			var results []map[string]interface{}
			for rows.Next() {
				values := make([]interface{}, len(columns))
				valuePtrs := make([]interface{}, len(columns))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				err := rows.Scan(valuePtrs...)
				if err != nil {
					log.Println("Error scanning row:", err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": "Error scanning row: " + err.Error()})
					return
				}
				row := make(map[string]interface{})
				for i, col := range columns {
					val := values[i]
					if b, ok := val.([]byte); ok {
						row[col] = string(b)
					} else {
						row[col] = val
					}
				}
				results = append(results, row)
			}
			c.JSON(http.StatusOK, gin.H{"message": "Query executed successfully", "data": results})
		} else {
			result, err := db.Exec(query)
			if err != nil {
				log.Println("Error executing query:", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "Error executing query: " + err.Error()})
				return
			}
			rowsAffected, _ := result.RowsAffected()

			// Broadcast the query to all slaves immediately (Synchronous Replication)
			mu.Lock()
			for _, slave := range slaves {
				_, err := slave.Write([]byte(dbName + "|" + query))
				if err != nil {
					fmt.Println("Error sending to Slave:", err)
				}
			}
			mu.Unlock()

			c.JSON(http.StatusOK, gin.H{"message": "Query executed successfully", "rows": rowsAffected})
		}
	})

	if err := r.Run(":8081"); err != nil {
		log.Fatal("Error starting frontend:", err)
	}
}
