package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestCopySchema(t *testing.T) {
	db, err := connectDB("mysql", "localhost", "3306", "root", "root")
	if err != nil {
		t.Fatal(err)
	}
	targetTableNames := map[string]struct{}{"aa": struct{}{}, "bb": struct{}{}, "cc": struct{}{}}
	targetSchemaName := fmt.Sprintf("target_sample_schema_%d", time.Now().UnixNano())
	tx, err := db.BeginTx(context.Background(), nil)
	defer tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", targetSchemaName))
	if err != nil {
		t.Fatal(err)
	}
	for name := range targetTableNames {
		_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s(test_column CHAR(1));", targetSchemaName, name))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", targetSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	sampleSchema := fmt.Sprintf("test_sample_schema_%d", time.Now().UnixNano())
	err = copySchema(context.Background(), db, targetSchemaName, sampleSchema, map[string]struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchema))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	rows, err := db.Query(fmt.Sprintf("SHOW TABLES FROM %s;", sampleSchema))
	if err != nil {
		t.Fatal(err)
	}
	sampleSchemaTables := []string{}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}
		sampleSchemaTables = append(sampleSchemaTables, tableName)
	}
	if len(sampleSchemaTables) != len(targetTableNames) {
		t.Log("table count not the same")
		t.FailNow()
	}
	for _, table := range sampleSchemaTables {
		_, exists := targetTableNames[table]
		if !exists {
			t.Logf("table %s does not exist in the original schema %s", table, targetSchemaName)
			t.FailNow()
		}
	}
}

func TestGetFowardRelationships(t *testing.T) {
	db, err := connectDB("mysql", "localhost", "3306", "root", "root")
	if err != nil {
		t.Fatal(err)
	}
	targetSchemaName := fmt.Sprintf("target_relationship_schema_%d", time.Now().UnixNano())
	tx, err := db.BeginTx(context.Background(), nil)
	defer tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", targetSchemaName))
	if err != nil {
		t.Fatal(err)
	}

	tbl1, tbl2 := "tbl1", "tbl2"
	_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s(c1 int primary key);", targetSchemaName, tbl1))
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s(c2 int, c3 int, FOREIGN KEY(c3) REFERENCES %s.%s(c1));", targetSchemaName, tbl2, targetSchemaName, tbl1))
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", targetSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	expectedRel := foreignKeyConstraint{
		table: tbl2, referencedTable: tbl1,
		tableCol: "c3", referencedTableCol: "c1",
	}

	rels, err := fowardRelationships(context.TODO(), db, targetSchemaName, tbl2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) <= 0 {
		t.Logf("got empty result set")
		t.FailNow()
	}
	if !reflect.DeepEqual(rels[0], expectedRel) {
		t.Logf("got %+v expected %+v", rels[0], expectedRel)
		t.FailNow()
	}
}

func TestGetReverseRelationships(t *testing.T) {
	db, err := connectDB("mysql", "localhost", "3306", "root", "root")
	if err != nil {
		t.Fatal(err)
	}
	targetSchemaName := fmt.Sprintf("target_relationship_schema_%d", time.Now().UnixNano())
	tx, err := db.BeginTx(context.Background(), nil)
	defer tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", targetSchemaName))
	if err != nil {
		t.Fatal(err)
	}

	tbl1, tbl2 := "tbl1", "tbl2"
	_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s(c1 int primary key);", targetSchemaName, tbl1))
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s(c2 int, c3 int, FOREIGN KEY(c3) REFERENCES %s.%s(c1));", targetSchemaName, tbl2, targetSchemaName, tbl1))
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", targetSchemaName))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()

	expectedRel := foreignKeyConstraint{
		table: tbl2, referencedTable: tbl1,
		tableCol: "c3", referencedTableCol: "c1",
	}

	rels, err := reverseRelationships(context.TODO(), db, targetSchemaName, tbl1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) <= 0 {
		t.Logf("got empty result set")
		t.FailNow()
	}
	if !reflect.DeepEqual(rels[0], expectedRel) {
		t.Logf("got %+v expected %+v", rels[0], expectedRel)
		t.FailNow()
	}
}

func TestSample(t *testing.T) {
	db, err := connectDB("mysql", "localhost", "3306", "root", "root")
	if err != nil {
		t.Fatal(err)
	}

	targetSchema, sampleSchema := "dev_vrp", fmt.Sprintf("test_sample_schema_%d", time.Now().Unix())
	err = copySchema(context.TODO(), db, targetSchema, sampleSchema, map[string]struct{}{})
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchema))
		if err != nil {
			log.Printf("err during clean up: %s\n", err)
		}
	}()
	if err != nil {
		log.Fatalf("could not copy schema: %s", err)
	}
	err = sample(context.TODO(), db, targetSchema, sampleSchema, "project_project", nil)
	if err != nil {
		t.Fatal(err)
	}

}

// func TestSample(t *testing.T) {
// 	db, err := connectDB("mysql", "localhost", "3308", "root", "root")
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	targetSchema, sampleSchema := "employees", fmt.Sprintf("test_sample_schema_%d", time.Now().Unix())
// 	err = copySchema(context.TODO(), db, targetSchema, sampleSchema, map[string]struct{}{})
// 	defer func() {
// 		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", sampleSchema))
// 		if err != nil {
// 			log.Printf("err during clean up: %s\n", err)
// 		}
// 	}()
// 	if err != nil {
// 		log.Fatalf("could not copy schema: %s", err)
// 	}
// 	err = sample(context.TODO(), db, targetSchema, sampleSchema, "employees", nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// }
