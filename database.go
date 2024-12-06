package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	Type   QueryType         `json:"type"`
	Table  string            `json:"table"`
	Fields []string          `json:"fields,omitempty"`
	Where  map[string]string `json:"where,omitempty"`
	Values map[string]string `json:"values,omitempty"`
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

	return nil
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	var queryRequest QueryRequest
	err := json.NewDecoder(r.Body).Decode(&queryRequest)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := validateQueryRequest(queryRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var query string
	var args []interface{}

	switch queryRequest.Type {
	case QueryTypeSelect:
		query, args = buildSelectQuery(queryRequest)
	case QueryTypeInsert:
		query, args = buildInsertQuery(queryRequest)
	case QueryTypeUpdate:
		query, args = buildUpdateQuery(queryRequest)
	case QueryTypeDelete:
		query, args = buildDeleteQuery(queryRequest)
	default:
		http.Error(w, "Invalid query type", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var result []map[string]interface{}
	columns, _ := rows.Columns()
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		err := rows.Scan(pointers...)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}
		row := make(map[string]interface{})
		for i, column := range columns {
			row[column] = values[i]
		}
		result = append(result, row)
	}

	json.NewEncoder(w).Encode(result)
}

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
	for col, val := range req.Values {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, i))
		args = append(args, val)
		i++
	}
	query := fmt.Sprintf("UPDATE %s SET %s", req.Table, strings.Join(setClauses, ", "))
	whereClause, whereArgs := buildWhereClause(req.Where)
	query += " WHERE " + whereClause
	args = append(args, whereArgs...)
	return query, args
}

func buildDeleteQuery(req QueryRequest) (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM %s", req.Table)
	whereClause, args := buildWhereClause(req.Where)
	query += " WHERE " + whereClause
	return query, args
}

func buildWhereClause(where map[string]string) (string, []interface{}) {
	var clauses []string
	var args []interface{}
	i := 1
	for col, val := range where {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", col, i))
		args = append(args, val)
		i++
	}
	return strings.Join(clauses, " AND "), args
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

// func handleInsertUser(w http.ResponseWriter, r *http.Request) {
// 	var request struct {
// 		Type   string `json:"type"`
// 		Table  string `json:"table"`
// 		Values struct {
// 			Email    string `json:"email"`
// 			Password string `json:"password"`
// 			R1       string `json:"R1"`
// 			R2       string `json:"R2"`
// 			R3       string `json:"R3"`
// 			R4       string `json:"R4"`
// 		} `json:"values"`
// 	}

// 	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
// 		http.Error(w, "Invalid request format", http.StatusBadRequest)
// 		return
// 	}

// 	// Convert string boolean to actual boolean
// 	r1, _ := strconv.ParseBool(request.Values.R1)
// 	r2, _ := strconv.ParseBool(request.Values.R2)
// 	r3, _ := strconv.ParseBool(request.Values.R3)
// 	r4, _ := strconv.ParseBool(request.Values.R4)

// 	query := `INSERT INTO users (email, password, R1, R2, R3, R4)
//               VALUES ($1, $2, $3, $4, $5, $6)`

// 	_, err := db.Exec(query,
// 		request.Values.Email,
// 		request.Values.Password,
// 		r1, r2, r3, r4)

// 	if err != nil {
// 		http.Error(w, "Failed to insert user", http.StatusInternalServerError)
// 		return
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(map[string]string{
// 		"message": "User inserted successfully",
// 	})
// }

// func handleViewUser(w http.ResponseWriter, r *http.Request) {
// 	var request struct {
// 		Type   string   `json:"type"`
// 		Table  string   `json:"table"`
// 		Fields []string `json:"fields"`
// 		Where  struct {
// 			Email string `json:"email"`
// 		} `json:"where"`
// 	}

// 	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
// 		http.Error(w, "Invalid request format", http.StatusBadRequest)
// 		return
// 	}

// 	query := `SELECT email, R1, R2, R3, R4 FROM users WHERE email = $1`
// 	row := db.QueryRow(query, request.Where.Email)

// 	var user struct {
// 		Email string `json:"email"`
// 		R1    bool   `json:"R1"`
// 		R2    bool   `json:"R2"`
// 		R3    bool   `json:"R3"`
// 		R4    bool   `json:"R4"`
// 	}

// 	if err := row.Scan(&user.Email, &user.R1, &user.R2, &user.R3, &user.R4); err != nil {
// 		http.Error(w, "User not found", http.StatusNotFound)
// 		return
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(user)
// }

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
