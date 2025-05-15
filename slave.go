package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

var (
	db         *sql.DB
	masterConn net.Conn
	mu         sync.Mutex
	shardMap   map[string]int
	shardDBs   []*sql.DB
	slaveID    int
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please provide the Master's IP address as argument (e.g., go run slave.go 192.168.1.100)")
	}
	masterIP := os.Args[1]

	var err error
	// Connect to local MySQL (will sync with Master later)
	db, err = sql.Open("mysql", "root:1234@tcp(127.0.0.1:3306)/")
	if err != nil {
		log.Fatal("Error connecting to local MySQL:", err)
	}
	defer db.Close()

	// Initialize shard databases and map
	shardMap = make(map[string]int) // Initialize the shard map
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

	// Connect to Master on port 8083
	masterConn, err = net.Dial("tcp", masterIP+":8083")
	if err != nil {
		log.Fatal("Error connecting to Master:", err)
	}
	defer masterConn.Close()

	fmt.Printf("Slave %d connected to Master at %s:8083\n", slaveID, masterIP)

	// Initial full sync with Master
	go fullSyncWithMaster()

	// Handle incoming commands from Master
	go handleMasterCommands()

	// Start Web Frontend on port 8082
	go startFrontend()

	// Keep the main goroutine alive
	select {}
}

func fullSyncWithMaster() {
	mu.Lock()
	defer mu.Unlock()

	// Request full sync from Master
	masterConn.Write([]byte("FULL_SYNC|"))
	buf := make([]byte, 4096) // Increased buffer size for full sync
	n, err := masterConn.Read(buf)
	if err != nil {
		log.Println("Error syncing with Master:", err)
		return
	}
	syncData := string(buf[:n])
	if strings.HasPrefix(syncData, "FULL_SYNC|") {
		syncData = strings.TrimPrefix(syncData, "FULL_SYNC|")
		queries := strings.Split(syncData, ";")
		for _, query := range queries {
			query = strings.TrimSpace(query)
			if query == "" {
				continue
			}
			_, err = db.Exec(query)
			if err != nil {
				log.Println("Error applying sync query:", err)
			} else {
				log.Println("Synced query:", query)
			}
		}
	}
}

func handleMasterCommands() {
	for {
		buf := make([]byte, 1024)
		n, err := masterConn.Read(buf)
		if err != nil {
			log.Println("Error reading from Master:", err)
			return
		}
		data := string(buf[:n])
		if data == "" {
			continue // Skip empty messages
		}
		parts := strings.SplitN(data, "|", 2)
		if len(parts) < 2 {
			log.Println("Received incomplete command from Master, waiting for valid command")
			continue
		}

		dbName := parts[0]
		query := parts[1]

		// Send acknowledgment for setup commands
		if dbName == "master" {
			_, err = db.Exec(query)
			if err != nil {
				log.Println("Error executing Master setup command:", err)
				masterConn.Write([]byte("Error: " + err.Error()))
			} else {
				masterConn.Write([]byte("OK"))
			}
			continue
		}

		queryType := strings.ToUpper(strings.Split(query, " ")[0])
		if queryType == "CREATE" || queryType == "DROP" {
			// Allow CREATE and DROP from Master
			_, err = db.Exec(query)
			if err != nil {
				log.Println("Error executing Master query:", err)
			} else {
				log.Println("Executed Master query:", query)
			}
			continue
		}

		// Execute query with proper sharding
		result, err := executeQueryWithSharding(query, dbName)
		if err != nil {
			log.Println("Error executing query:", err)
		} else {
			log.Println("Executed query:", query, "Result:", result)
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
				// Remove any backticks or quotes from table name
				tableName = strings.Trim(tableName, "`\"'")
				break
			}
		}

		if tableName != "" {
			mu.Lock() // Lock when accessing/modifying shardMap
			shardID, exists := shardMap[tableName]
			if !exists {
				// For new tables, assign to shard 0 by default
				shardID = 0
				shardMap[tableName] = shardID
				log.Printf("Assigned new table %s to Shard %d\n", tableName, shardID)
			}
			mu.Unlock()

			if shardID >= 0 && shardID < len(shardDBs) {
				targetDB = shardDBs[shardID]
				log.Printf("Executing %s on Shard %d\n", query, shardID)
				_, err := targetDB.Exec("USE " + dbName)
				if err != nil {
					log.Printf("Error selecting database %s on shard %d: %v\n", dbName, shardID, err)
					return "", err
				}
			} else {
				log.Printf("Invalid shard ID %d for table %s, using default database\n", shardID, tableName)
			}
		}
	}

	result, err := targetDB.Exec(query)
	if err != nil {
		log.Printf("Error executing query on shard %d: %s\nError: %v\n", shardID, query, err)
		return "", err
	}
	rowsAffected, _ := result.RowsAffected()
	return fmt.Sprintf("Rows affected: %d", rowsAffected), nil
}

func startFrontend() {
	r := gin.Default()
	r.LoadHTMLGlob("templates/*.html")
	r.Static("/static", "./static")

	r.GET("/", func(c *gin.Context) {
		userType := c.Query("user")
		c.HTML(http.StatusOK, "index.html", gin.H{
			"IsMaster": userType == "master",
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
				rows.Scan(valuePtrs...)
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
			// Execute the query locally first
			result, err := db.Exec(query)
			if err != nil {
				log.Println("Error executing query:", err)
				c.JSON(http.StatusBadRequest, gin.H{"error": "Error executing query: " + err.Error()})
				return
			}
			rowsAffected, _ := result.RowsAffected()

			// Send the query to Master immediately (Synchronous Replication)
			_, err = masterConn.Write([]byte(dbName + "|" + query))
			if err != nil {
				log.Println("Error sending query to Master:", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Error sending query to Master: " + err.Error()})
				return
			}

			c.JSON(http.StatusOK, gin.H{"message": "Query executed successfully", "rows": rowsAffected})
		}
	})

	if err := r.Run(":8082"); err != nil {
		log.Fatal("Error starting frontend:", err)
	}
}
