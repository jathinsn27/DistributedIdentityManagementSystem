package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

type QueryType string

const (
	QueryTypeSelect QueryType = "SELECT"
	QueryTypeInsert QueryType = "INSERT"
	QueryTypeUpdate QueryType = "UPDATE"
	QueryTypeDelete QueryType = "DELETE"
)

type QueryRequest struct {
	Type      QueryType         `json:"type"`
	Table     string            `json:"table"`
	Fields    []string          `json:"fields,omitempty"`
	Where     map[string]string `json:"where,omitempty"`
	Values    map[string]string `json:"values,omitempty"`
	DeleteAll bool
}

var db *sql.DB

func initDB() error {
	var err error
	connStr := fmt.Sprintf("host=db-%s user=postgres password=password dbname=nodedb sslmode=disable", os.Getenv("NODE_ID"))
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("error pinging database: %v", err)
	}
	fmt.Println("Successfully connected to database")

	var tableExists bool
	err = db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')").Scan(&tableExists)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}

	var tableExistsLog bool
	err = db.QueryRow("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transaction_log')").Scan(&tableExistsLog)
	if err != nil {
		return fmt.Errorf("error checking if table exists: %v", err)
	}

	if !tableExists {
		_, err = db.Exec(`
			CREATE TABLE users (
				email VARCHAR(255) PRIMARY KEY,
				password CHAR(64),
				R1 BOOLEAN,
				R2 BOOLEAN,
				R3 BOOLEAN,
				R4 BOOLEAN
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating users table: %v", err)
		}
		fmt.Println("Created users table")
	} else {
		fmt.Println("Users table already exists")
	}

	if !tableExistsLog {
		_, err = db.Exec(`
			CREATE TABLE transaction_log (
				id SERIAL PRIMARY KEY,
				type VARCHAR(10),
				table_name VARCHAR(255),
				query TEXT,
				timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating transactions table: %v", err)
		}
		fmt.Println("Created transaction_log table")
	} else {
		fmt.Println("Transactions table already exists")
	}

	return nil
}

func logTransaction(queryType QueryType, table string, query string, params ...interface{}) error {
	// Format the query with its parameters for logging
	formattedQuery := formatQueryWithParams(query, params)

	// Log the formatted query
	_, err := db.Exec("INSERT INTO transaction_log (type, table_name, query) VALUES ($1, $2, $3)", queryType, table, formattedQuery)
	return err
}

// formatQueryWithParams formats a SQL query string with its parameters.
func formatQueryWithParams(query string, params []interface{}) string {
	for i, param := range params {
		placeholder := fmt.Sprintf("$%d", i+1)
		var value string

		switch v := param.(type) {
		case string:
			value = fmt.Sprintf("'%s'", v)
		case bool:
			value = fmt.Sprintf("%t", v) // Use boolean as `true`/`false`
		default:
			value = fmt.Sprintf("%v", v)
		}

		query = strings.Replace(query, placeholder, value, 1)
	}
	return query
}

func requestMissingLogs(leaderAddress string, lastID int) ([]map[string]interface{}, error) {
	fmt.Printf("http://%s/logs?last_id=%d", leaderAddress, lastID)
	resp, err := http.Get(fmt.Sprintf("http://%s/logs?last_id=%d", leaderAddress, lastID))
	if err != nil {
		return nil, fmt.Errorf("error requesting logs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("error response from leader: %s", resp.Status)
	}

	var logs []map[string]interface{}
	body, _ := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, &logs)
	if err != nil {
		return nil, fmt.Errorf("error decoding logs: %v", err)
	}

	return logs, nil
}

func getLogsAfter(lastID int) ([]map[string]interface{}, error) {
	query := "SELECT id, type, table_name, query FROM transaction_log WHERE id > $1 ORDER BY id ASC"

	rows, err := db.Query(query, lastID)
	if err != nil {
		return nil, fmt.Errorf("error querying transaction logs: %v", err)
	}
	defer rows.Close()

	var logs []map[string]interface{}
	for rows.Next() {
		var id int
		var ttype, tableName, query string

		if err := rows.Scan(&id, &ttype, &tableName, &query); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		logEntry := map[string]interface{}{
			"id":         id,
			"type":       ttype,
			"table_name": tableName,
			"query":      query,
		}
		logs = append(logs, logEntry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %v", err)
	}

	return logs, nil
}

func applyLogs(logs []map[string]interface{}) error {
	for _, logEntry := range logs {
		query := logEntry["query"].(string)
		fmt.Printf("Applying log entry: %s\n", query)
		// log the query.
		_, err := db.Exec("INSERT INTO transaction_log (type, table_name, query) VALUES ($1, $2, $3)", logEntry["type"], logEntry["table"], logEntry["query"])
		if err != nil {
			return nil
		}
		// Execute the query on the local database
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("error applying log entry: %v", err)
		}
	}
	return nil
}

func getLastProcessedID() (int, error) {
	var lastID int
	err := db.QueryRow("SELECT COALESCE(MAX(id), 0) FROM transaction_log").Scan(&lastID)
	if err != nil {
		return 0, fmt.Errorf("error retrieving last processed ID: %v", err)
	}
	return lastID, nil
}

// func handleQuery(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	var queryRequest QueryRequest
// 	err := json.NewDecoder(r.Body).Decode(&queryRequest)
// 	if err != nil {
// 		http.Error(w, "Invalid JSON", http.StatusBadRequest)
// 		return
// 	}

// 	if err := validateQueryRequest(queryRequest); err != nil {
// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 		return
// 	}

// 	var query string
// 	var args []interface{}

// 	// Build the query and log the transaction
// 	switch queryRequest.Type {
// 	case QueryTypeSelect:
// 		query, args = buildSelectQuery(queryRequest)
// 	case QueryTypeInsert:
// 		query, args = buildInsertQuery(queryRequest)
// 		logTransaction(QueryTypeInsert, queryRequest.Table, query, args...)
// 	case QueryTypeUpdate:
// 		query, args = buildUpdateQuery(queryRequest)
// 		logTransaction(QueryTypeUpdate, queryRequest.Table, query, args...)
// 	case QueryTypeDelete:
// 		query, args = buildDeleteQuery(queryRequest)
// 		logTransaction(QueryTypeDelete, queryRequest.Table, query, args...)
// 	default:
// 		http.Error(w, "Invalid query type", http.StatusBadRequest)
// 		return
// 	}

// 	// Debug logging
// 	fmt.Printf("Executing query: %s\nWith args: %v\n", query, args)

// 	// Handle SELECT queries
// 	if queryRequest.Type == QueryTypeSelect {
// 		rows, err := db.Query(query, args...)
// 		if err != nil {
// 			http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
// 			return
// 		}
// 		defer rows.Close()

// 		var result []map[string]interface{}
// 		columns, _ := rows.Columns()
// 		for rows.Next() {
// 			values := make([]interface{}, len(columns))
// 			pointers := make([]interface{}, len(columns))
// 			for i := range values {
// 				pointers[i] = &values[i]
// 			}
// 			err := rows.Scan(pointers...)
// 			if err != nil {
// 				http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
// 				return
// 			}
// 			row := make(map[string]interface{})
// 			for i, column := range columns {
// 				row[column] = values[i]
// 			}
// 			result = append(result, row)
// 		}

// 		json.NewEncoder(w).Encode(result)
// 		return
// 	}

// 	// Handle INSERT, UPDATE, DELETE queries
// 	result, err := db.Exec(query, args...)
// 	if err != nil {
// 		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
// 		return
// 	}

// 	rowCount, err := result.RowsAffected()
// 	if err != nil {
// 		http.Error(w, fmt.Sprintf("Error fetching rows affected: %v", err), http.StatusInternalServerError)
// 		return
// 	}

// 	// Return a success response with rows affected
// 	w.Header().Set("Content-Type", "application/json")
// 	response := map[string]interface{}{
// 		"message":       "Query executed successfully",
// 		"rows_affected": rowCount,
// 	}
// 	json.NewEncoder(w).Encode(response)
// }

func validateQueryRequest(req QueryRequest) error {
	if req.Table == "" {
		return fmt.Errorf("table name is required")
	}
	switch req.Type {
	case QueryTypeSelect:
		if len(req.Fields) == 0 {
			return fmt.Errorf("at least one field is required for SELECT")
		}
	case QueryTypeInsert:
		if len(req.Values) == 0 {
			return fmt.Errorf("values are required for INSERT")
		}
	case QueryTypeUpdate:
		if len(req.Values) == 0 || len(req.Where) == 0 {
			return fmt.Errorf("values and where clause are required for UPDATE")
		}
	case QueryTypeDelete:
		if len(req.Where) == 0 {
			return fmt.Errorf("where clause is required for DELETE")
		}
	default:
		return fmt.Errorf("invalid query type")
	}
	return nil
}

func buildSelectQuery(req QueryRequest) (string, []interface{}) {
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(req.Fields, ", "), req.Table)
	var args []interface{}
	if len(req.Where) > 0 {
		whereClause, whereArgs := buildWhereClause(req.Where)
		query += " WHERE " + whereClause
		args = whereArgs
	}
	return query, args
}

func buildInsertQuery(req QueryRequest) (string, []interface{}) {
	var columns, placeholders []string
	var args []interface{}
	i := 1
	for col, val := range req.Values {
		columns = append(columns, col)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, val)
		i++
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		req.Table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return query, args
}

func buildUpdateQuery(req QueryRequest) (string, []interface{}) {
	var setClauses []string
	var args []interface{}
	i := 1

	// Build the SET clause
	for col, val := range req.Values {
		if col == "R1" || col == "R2" || col == "R3" || col == "R4" {
			// Explicitly convert string to boolean
			boolVal := strings.ToLower(fmt.Sprintf("%v", val)) == "true"
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, i))
			args = append(args, boolVal) // Append boolean
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, i))
			args = append(args, val) // Append as-is
		}
		i++
	}

	// Build the WHERE clause, continuing index from SET
	whereClause, whereArgs := buildWhereClause(req.Where, i)

	// Append the placeholders for WHERE clause
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", req.Table, strings.Join(setClauses, ", "), whereClause)
	args = append(args, whereArgs...) // Append WHERE clause arguments

	return query, args
}

func buildDeleteQuery(req QueryRequest) (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM %s", req.Table)
	whereClause, args := buildWhereClause(req.Where)
	query += " WHERE " + whereClause
	return query, args
}

func buildWhereClause(where map[string]string, startIndex ...int) (string, []interface{}) {
	var clauses []string
	var args []interface{}

	// Determine the starting index; default to 1 if not provided
	i := 1
	if len(startIndex) > 0 {
		i = startIndex[0]
	}

	// Build WHERE clause
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", col, i))
		args = append(args, val)
		i++
	}

	return strings.Join(clauses, " AND "), args
}

func handleUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var queryRequest QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&queryRequest); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	queryRequest.Type = QueryTypeSelect
	if err := validateQueryRequest(queryRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query, args := buildSelectQuery(queryRequest)
	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	columns, _ := rows.Columns()

	if rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		row := make(map[string]interface{})
		for i, column := range columns {
			row[column] = values[i]
		}
		result = append(result, row)
	} else {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleInsertUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var queryRequest QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&queryRequest); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if queryRequest.Type != QueryTypeInsert {
		http.Error(w, "Invalid query type", http.StatusBadRequest)
		return
	}

	query, args := buildInsertQuery(queryRequest)
	_, err := db.Exec(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "User inserted successfully",
	})
}

func handleViewAllUser(w http.ResponseWriter, r *http.Request) {
	query := `SELECT email, R1, R2, R3, R4 FROM users`
	rows, err := db.Query(query)
	if err != nil {
		http.Error(w, "Failed to fetch users", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []struct {
		Email string `json:"email"`
		R1    bool   `json:"R1"`
		R2    bool   `json:"R2"`
		R3    bool   `json:"R3"`
		R4    bool   `json:"R4"`
	}

	for rows.Next() {
		var user struct {
			Email string `json:"email"`
			R1    bool   `json:"R1"`
			R2    bool   `json:"R2"`
			R3    bool   `json:"R3"`
			R4    bool   `json:"R4"`
		}
		if err := rows.Scan(&user.Email, &user.R1, &user.R2, &user.R3, &user.R4); err != nil {
			http.Error(w, "Error reading user data", http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(users)
}

func handleUpdatePermissions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var queryRequest QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&queryRequest); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	queryRequest.Type = QueryTypeUpdate
	queryRequest.Table = "users"

	// Validate the request using existing validation function
	if err := validateQueryRequest(queryRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use the existing query builder
	query, args := buildUpdateQuery(queryRequest)
	result, err := db.Exec(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Permissions updated successfully",
	})
}
