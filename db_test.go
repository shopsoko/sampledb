package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	DATABASE_HOST = "localhost"
	DATABASE_PORT = "3306"
	DATABASE_USER = "root"
	DATABASE_PASS = "root"
)

func TestMain(m *testing.M) {
	if os.Getenv("DATABASE_HOST") != "" {
		DATABASE_HOST = os.Getenv("DATABASE_HOST")
	}
	if os.Getenv("DATABASE_PORT") != "" {
		DATABASE_PORT = os.Getenv("DATABASE_PORT")
	}
	if os.Getenv("DATABASE_USER") != "" {
		DATABASE_USER = os.Getenv("DATABASE_USER")
	}
	if os.Getenv("DATABASE_PASS") != "" {
		DATABASE_PASS = os.Getenv("DATABASE_PASS")
	}
	os.Exit(m.Run())
}

func TestCopySchema(t *testing.T) {
	db, err := connectDB("mysql", DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASS)
	if err != nil {
		t.Fatal(err)
	}
	sampleSchemaName := "copyschema_test"
	testDataPath := filepath.Join("test-fixtures", "copyschema.sql")
	testData, err := ioutil.ReadFile(testDataPath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(string(testData))
	if err != nil {
		t.Fatal(err)
	}
	// clean up
	defer func() {
		testData, err := ioutil.ReadFile(filepath.Join("test-fixtures", "copyschema_down.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(string(testData))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	err = copySchema(context.Background(), db, "copyschema", sampleSchemaName, map[string]struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	sampleSchemaRows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM %s;", sampleSchemaName))
	if err != nil {
		t.Fatal(err)
	}
	targetSchemaRows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM %s;", sampleSchemaName))
	if err != nil {
		t.Fatal(err)
	}
	targetSchemaNames, sampleSchemaTables := []string{}, []string{}
	for sampleSchemaRows.Next() {
		var tableName string
		err = sampleSchemaRows.Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}
		sampleSchemaTables = append(sampleSchemaTables, tableName)
	}
	for targetSchemaRows.Next() {
		var tableName string
		err = targetSchemaRows.Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}
		targetSchemaNames = append(targetSchemaNames, tableName)
	}
	if len(sampleSchemaTables) != len(targetSchemaNames) {
		t.Log("table count not the same")
		t.FailNow()
	}
	if !reflect.DeepEqual(targetSchemaNames, sampleSchemaTables) {
		t.FailNow()
	}
}

func TestGetFowardRelationships(t *testing.T) {
	db, err := connectDB("mysql", DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASS)
	if err != nil {
		t.Fatal(err)
	}
	testDataPath := filepath.Join("test-fixtures", "foward.sql")
	testData, err := ioutil.ReadFile(testDataPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(testData))
	if err != nil {
		t.Fatal(err)
	}
	// clean up
	defer func() {
		testData, err := ioutil.ReadFile(filepath.Join("test-fixtures", "foward_down.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(string(testData))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	dataBytes, err := ioutil.ReadFile(filepath.Join("test-fixtures", "foward_rels.json"))
	if err != nil {
		t.Fatal(err)
	}

	type dataStruct struct {
		Table string `json:"table"`
		Rels  []struct {
			Column   string `json:"column"`
			RefCol   string `json:"referenced_column"`
			RefTable string `json:"referenced_table"`
		} `json:"foward_rels"`
	}
	dts := []dataStruct{}
	err = json.Unmarshal(dataBytes, &dts)
	if err != nil {
		t.Fatal(err)
	}
	for _, tableRels := range dts {
		rels, err := fowardRelationships(context.TODO(), db, "foward", tableRels.Table)
		if err != nil {
			t.Fatal(err)
		}
		expected := []foreignKeyConstraint{}
		for _, rel := range tableRels.Rels {
			expected = append(expected, foreignKeyConstraint{referencedTable: rel.RefTable, referencedTableCol: rel.RefCol, tableCol: rel.Column, table: tableRels.Table})
		}
		if !reflect.DeepEqual(expected, rels) {
			t.FailNow()
		}

	}
}

func TestGetReverseRelationships(t *testing.T) {
	db, err := connectDB("mysql", DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASS)
	if err != nil {
		t.Fatal(err)
	}
	testDataPath := filepath.Join("test-fixtures", "reverse.sql")
	testData, err := ioutil.ReadFile(testDataPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(testData))
	if err != nil {
		t.Fatal(err)
	}
	// clean up
	defer func() {
		testData, err := ioutil.ReadFile(filepath.Join("test-fixtures", "reverse_down.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(string(testData))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	dataBytes, err := ioutil.ReadFile(filepath.Join("test-fixtures", "reverse_rels.json"))
	if err != nil {
		t.Fatal(err)
	}

	type dataStruct struct {
		Table string `json:"table"`
		Rels  []struct {
			Column string `json:"column"`
			Table  string `json:"table"`
			RefCol string `json:"referenced_column"`
		} `json:"reverse_rels"`
	}
	dts := []dataStruct{}
	err = json.Unmarshal(dataBytes, &dts)
	if err != nil {
		t.Fatal(err)
	}

	for _, tableRels := range dts {
		rels, err := reverseRelationships(context.TODO(), db, "reverse", tableRels.Table)
		if err != nil {
			t.Fatal(err)
		}
		expected := []foreignKeyConstraint{}
		for _, rel := range tableRels.Rels {
			expected = append(expected, foreignKeyConstraint{referencedTable: tableRels.Table, referencedTableCol: rel.RefCol, tableCol: rel.Column, table: rel.Table})
		}
		if !reflect.DeepEqual(expected, rels) {
			t.FailNow()
		}
	}
}

func TestInsertRowFowardRels(t *testing.T) {
	db, err := connectDB("mysql", DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASS)
	if err != nil {
		t.Fatal(err)
	}
	testDataPath := filepath.Join("test-fixtures", "insert_foward.sql")
	testData, err := ioutil.ReadFile(testDataPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(testData))
	if err != nil {
		t.Fatal(err)
	}
	sampleSchemaName := "insert_foward_test"
	err = copySchema(context.Background(), db, "insert_foward", sampleSchemaName, map[string]struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	// clean up
	defer func() {
		testData, err := ioutil.ReadFile(filepath.Join("test-fixtures", "insert_foward_down.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(string(testData))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	dataBytes, err := ioutil.ReadFile(filepath.Join("test-fixtures", "insert_foward.json"))
	if err != nil {
		t.Fatal(err)
	}
	type dataStruct struct {
		Table  string `json:"table"`
		PkData []struct {
			Column string      `json:"column"`
			Data   interface{} `json:"data"`
		} `json:"pk"`
		Rels []struct {
			Column string      `json:"column"`
			Table  string      `json:"referenced_table"`
			RefCol string      `json:"referenced_column"`
			Data   interface{} `json:"data"`
		} `json:"rels"`
	}
	dts := []dataStruct{}
	err = json.Unmarshal(dataBytes, &dts)
	if err != nil {
		t.Fatal(err)
	}

	dbx := sqlx.NewDb(db, "mysql")
	for _, tableRels := range dts {
		rels, err := fowardRelationships(context.TODO(), db, "insert_foward", tableRels.Table)
		if err != nil {
			t.Fatal(err)
		}
		var pkWhereClause string
		for i, pk := range tableRels.PkData {
			pkWhereClause += fmt.Sprintf("`%s` = '%s'", pk.Column, pk.Data)
			if i < len(tableRels.PkData)-1 {
				pkWhereClause += " AND "
			}
		}
		row, err := dbx.Queryx(fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", "insert_foward", tableRels.Table, pkWhereClause))
		if err != nil {
			t.Fatal(err)
		}
		for row.Next() {
			data := make(map[string]interface{})
			row.MapScan(data)
			err = insertRowFowardRels(context.TODO(), dbx, "insert_foward", sampleSchemaName, rels, data)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, rel := range tableRels.Rels {
			row, err := dbx.Query(fmt.Sprintf("SELECT %s FROM %s.%s WHERE `%s` = ?;", rel.RefCol, sampleSchemaName, rel.Table, rel.RefCol), rel.Data)
			if err != nil {
				t.Fatal(err)
			}
			if !row.Next() {
				t.Log("could not find desired row")
				t.FailNow()
			}
		}
	}

}

func TestSample(t *testing.T) {
	db, err := connectDB("mysql", DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASS)
	if err != nil {
		t.Fatal(err)
	}

	testDataPath := filepath.Join("test-fixtures", "sample.sql")
	testData, err := ioutil.ReadFile(testDataPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(testData))
	if err != nil {
		t.Fatal(err)
	}

	targetSchema, sampleSchemaName := "sample", fmt.Sprintf("test_sample_schema_%d", time.Now().Unix())
	err = copySchema(context.TODO(), db, targetSchema, sampleSchemaName, map[string]struct{}{})
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
		testData, err := ioutil.ReadFile(filepath.Join("test-fixtures", "sample_down.sql"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(string(testData))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()
	if err != nil {
		log.Fatalf("could not copy schema: %s", err)
	}

	type sampleStruct struct {
		Sampled string   `json:"sampled"`
		Column  string   `json:"column"`
		Data    []string `json:"data"`
		Deps    []struct {
			Table string `json:"table"`
			Rows  []struct {
				Columns []string `json:"columns"`
				Data    []string `json:"data"`
			} `json:"rows"`
		} `json:"deps"`
	}

	testDataVal, err := ioutil.ReadFile(filepath.Join("test-fixtures", "sample.json"))
	if err != nil {
		t.Fatal(err)
	}

	datas := []sampleStruct{}
	err = json.Unmarshal(testDataVal, &datas)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range datas {
		paramData := []interface{}{}
		for _, d := range data.Data {
			paramData = append(paramData, d)
		}

		params := &sampleParams{table: data.Sampled, column: data.Column, data: paramData}
		err = sample(context.TODO(), db, targetSchema, sampleSchemaName, params)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query(makeSampleQuery(sampleSchemaName, params))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.FailNow()
		}
		for rows.Next() {
			var pk string
			err = rows.Scan(&pk)
			if err != nil {
				t.Fatal(err)
			}
			if pk != params.data[0] {
				t.FailNow()
			}
		}

		for _, dep := range data.Deps {
			var whereClause string
			for _, row := range dep.Rows {
				for idx, col := range row.Columns {
					whereClause += fmt.Sprintf("`%s` = '%s'", col, row.Data[idx])
					if idx < len(row.Columns)-1 {
						whereClause += " AND "
					}
				}
			}
			rowData, err := db.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE %s;", sampleSchemaName, dep.Table, whereClause))
			if err != nil {
				t.Fatal(err)
			}
			defer rowData.Close()
			if !rowData.Next() {
				t.FailNow()
			}
			for rowData.Next() {
				var count int
				err = rows.Scan(&count)
				if err != nil {
					t.Fatal(err)
				}
				if count != len(dep.Rows) {
					t.FailNow()
				}
			}
		}
	}
}
