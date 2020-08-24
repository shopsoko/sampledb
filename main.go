package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// usage: sampledb -driver=dbdriver -host=dbhost -port=port -user=user -pass -targetschema=targetschema -sampleschema=sampleschema -anchor=table_name -nosample=tbl1,tbl2
func main() {
	driver := flag.String("driver", "mysql", "db driver")
	host := flag.String("host", "localhost", "db host")
	port := flag.String("port", "3306", "db port")
	user := flag.String("user", "root", "db user")
	pass := flag.String("pass", "root", "db user pass")
	targetSchema := flag.String("targetschema", "", "target schema name")
	sampleSchema := flag.String("sampleschema", "defaults to sample_db_{secs since January 1, 1970 UTC}", "sample schema name")
	anchorTable := flag.String("anchor", "", "table from which we'll start looking fot relationships")
	noSampleTable := flag.String("nosample", "", "comma separated list of tables name which will be copied in full")

	flag.Parse()

	// where we'll copy our sampled data
	var sampleSchemaName string
	if sampleSchema != nil {
		sampleSchemaName = *sampleSchema
	} else {
		sampleSchemaName = fmt.Sprintf("sample_db_%d", time.Now().Unix())

	}

	db, err := connectDB(*driver, *host, *port, *user, *pass)
	if err != nil {
		log.Fatalf("could not connect to db: %s", err)
	}

	noSmplTbls := map[string]struct{}{}
	if *noSampleTable != "" {
		tbls := strings.Split(*noSampleTable, ",")
		for _, tbl := range tbls {
			noSmplTbls[tbl] = struct{}{}
		}
	}
	err = copySchema(context.TODO(), db, *targetSchema, sampleSchemaName, noSmplTbls)
	if err != nil {
		log.Fatalf("could not copy schema: %s", err)
	}

	for whitelistedTable := range noSmplTbls {
		err = sample(context.TODO(), db, *targetSchema, sampleSchemaName, whitelistedTable)
		if err != nil {
			log.Fatalf("could not sample db: %s", err)
		}
	}

	err = sample(context.TODO(), db, *targetSchema, sampleSchemaName, *anchorTable)
	if err != nil {
		log.Fatalf("could not sample db: %s", err)
	}
}

func connectDB(driver, host, port, user, pass string) (*sql.DB, error) {
	return sql.Open(driver, fmt.Sprintf("%s:%s@tcp(%s:%s)/", user, pass, host, port))
}

// perms: requires SHOW VIEW privilege
func copySchema(ctx context.Context, db *sql.DB, targetSchema, sampleSchema string, noSampleTables map[string]struct{}) error {
	rows, err := db.Query(fmt.Sprintf("SHOW FULL TABLES FROM %s;", targetSchema))
	if err != nil {
		return fmt.Errorf("show tables: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("rollback err: %s, %w", tx.Rollback(), err)
	}
	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE %s;", sampleSchema))
	if err != nil {
		return fmt.Errorf("rollback err: %s, create db: %w", tx.Rollback(), err)
	}
	// we create the views after creating the tables
	views := []string{}
	for rows.Next() {
		var tableName, tableType string
		err = rows.Scan(&tableName, &tableType)
		if err != nil {
			return err
		}
		switch tableType {
		case "VIEW":
			views = append(views, tableName)
		case "BASE TABLE":
			_, err = tx.Exec(fmt.Sprintf("CREATE TABLE %s.%s LIKE %s.%s;", sampleSchema, tableName, targetSchema, tableName))
			if err != nil {
				return fmt.Errorf("create table %s: %w", tableName, err)
			}
			if _, exists := noSampleTables[tableName]; exists {
				_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.%s;", sampleSchema, tableName, targetSchema, tableName))
				if err != nil {
					return fmt.Errorf("create table %s: %w", tableName, err)
				}
			}
		default:
			return fmt.Errorf("unknown table type %s", tableType)
		}
	}
	for _, viewName := range views {
		rows, err := db.Query(fmt.Sprintf("SELECT view_definition FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s';", targetSchema, viewName))
		if err != nil {
			return fmt.Errorf("show view definition err: %w", err)
		}
		if !rows.Next() {
			return fmt.Errorf("could not find view definition: %s", viewName)
		}
		var definition string
		err = rows.Scan(&definition)
		if err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, definition)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

type foreignKeyRel struct {
	table              string
	referencedTable    string
	tableCol           string
	referencedTableCol string
	reverse            bool
}

// returns columns and the tables that are referenced by the targetTable via FOREIGN KEY constraints
func fowardRelationships(ctx context.Context, db *sql.DB, targetTable string) ([]foreignKeyRel, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT column_name, referenced_column_name, referenced_table_name FROM information_schema.key_column_usage WHERE table_name = '%s' AND referenced_table_name != 'NULL';",
			targetTable))
	if err != nil {
		return nil, err
	}
	cols := map[string]struct{}{}
	rels := []foreignKeyRel{}
	for rows.Next() {
		var colName, refColName, refTableName string
		err = rows.Scan(&colName, &refColName, &refTableName)
		if err != nil {
			return nil, err
		}
		if _, exists := cols[colName]; !exists {
			cols[colName] = struct{}{}
			rels = append(rels, foreignKeyRel{
				table: targetTable, referencedTable: refTableName,
				tableCol: colName, referencedTableCol: refColName,
			})
		}
	}
	return rels, nil
}

// returns columns and the tables that reference the targetTable via FOREIGN KEY constraints
func reverseRelationships(ctx context.Context, db *sql.DB, targetTable string) ([]foreignKeyRel, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT table_name, column_name, referenced_column_name FROM information_schema.key_column_usage WHERE referenced_table_name = '%s';",
			targetTable))
	if err != nil {
		return nil, err
	}
	rels := []foreignKeyRel{}
	for rows.Next() {
		var colName, refColName, tableName string
		err = rows.Scan(&tableName, &colName, &refColName)
		if err != nil {
			return nil, err
		}
		rels = append(rels, foreignKeyRel{
			table: tableName, referencedTable: targetTable,
			tableCol: colName, referencedTableCol: refColName,
		})
	}
	return rels, nil
}

func makeInsertQuery(rowdata map[string]interface{}, db *sql.DB, schema, table string) (string, []interface{}, error) {
	if len(rowdata) == 0 {
		return "", nil, sql.ErrNoRows
	}
	values, cols := "", ""
	valArray := []interface{}{}
	for k, v := range rowdata {
		values += "?,"
		valArray = append(valArray, v)
		cols += fmt.Sprintf("`%s`,", k)
	}
	return fmt.Sprintf("INSERT IGNORE INTO %s.%s (%s) VALUES (%s);", schema, table, cols[:len(cols)-1], values[:len(values)-1]), valArray, nil
}

func getAllDeps(ctx context.Context, dbx *sqlx.DB, targetSchema, sampleSchema string, rel foreignKeyRel, parentRow map[string]interface{}) error {
	// get all rows that reference the parentRow
	q := fmt.Sprintf("SELECT * FROM %s.%s WHERE `%s` = '%s';", targetSchema, rel.table, rel.tableCol, parentRow[rel.referencedTableCol])
	log.Printf("1 #%s query: %s", parentRow["id"], q)
	rows, err := dbx.QueryxContext(ctx, q)
	if err != nil {
		return err
	}
	fowardRels, err := fowardRelationships(ctx, dbx.DB, rel.table)
	if err != nil {
		return err
	}

	revRes, err := reverseRelationships(ctx, dbx.DB, rel.table)
	if err != nil {
		return err
	}

	dedupQuery := make(map[string]struct{})
	for rows.Next() {
		rowData := make(map[string]interface{})
		rows.MapScan(rowData)
		// rowData["aoi/op"] = rel.table

		stmts := []map[string][]interface{}{}
		stmt, vals, err := makeInsertQuery(rowData, dbx.DB, sampleSchema, rel.table)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if stmt != "" {
			insertQuery := map[string][]interface{}{stmt: vals}
			stmts = append([]map[string][]interface{}{insertQuery}, stmts...)
		}

		for i := 0; i < len(fowardRels); i++ {
			// log.Printf("#%s foward rel: vals [ %+s ]  [ %+s ]\n", rowData["id"], rowData[fowardRels[i].tableCol], rowData)
			if d := rowData[fowardRels[i].tableCol]; d != nil {
				q := fmt.Sprintf("SELECT * FROM %s.%s WHERE `%s` = '%s';", targetSchema, fowardRels[i].referencedTable, fowardRels[i].referencedTableCol, d)
				if _, exists := dedupQuery[q]; !exists {
					log.Printf("foward %+v\n", fowardRels[i])
					log.Printf("2 #%s query: %s, referenced %s", rowData["id"], q, fowardRels[i].tableCol)
					dedupQuery[q] = struct{}{}
					r, err := dbx.QueryxContext(ctx, q)
					if err != nil {
						return err
					}
					for r.Next() {
						rd := make(map[string]interface{})
						r.MapScan(rd)
						stmt, vals, err := makeInsertQuery(rd, dbx.DB, sampleSchema, fowardRels[i].referencedTable)
						if err != nil && !errors.Is(err, sql.ErrNoRows) {
							return err
						}
						if stmt != "" {
							insertQuery := map[string][]interface{}{stmt: vals}
							stmts = append([]map[string][]interface{}{insertQuery}, stmts...)
						}
					}
				}
			}
			tx, err := dbx.DB.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			for _, query := range stmts {
				for q, vals := range query {
					// log.Printf("insert %s :: row %+s\n", q, vals)
					_, err := tx.ExecContext(ctx, q, vals...)
					if err != nil {
						return fmt.Errorf("tx rolled back %s: %w query { %s row %+s }", tx.Rollback(), err, q, vals)
					}
				}
			}
			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("could not commit tx: %w", err)
			}
		}

		for _, rel := range revRes {
			q := fmt.Sprintf("SELECT * FROM %s.%s WHERE `%s` = '%s';", targetSchema, rel.table, rel.tableCol, rowData[rel.referencedTableCol])
			if _, exists := dedupQuery[q]; !exists {
				log.Printf("rev %+v\n", rel)
				log.Printf("3 #%s query: %s", rowData["id"], q)
				dedupQuery[q] = struct{}{}
				rows, err := dbx.QueryxContext(ctx, q)
				if err != nil {
					return err
				}
				for rows.Next() {
					rowData := make(map[string]interface{})
					rows.MapScan(rowData)
					// rowData["aoi/op"] = rel.table
					err = getAllDeps(ctx, dbx, targetSchema, sampleSchema, rel, rowData)
					return err
				}
			}
		}

	}

	return nil
}

func sample(ctx context.Context, db *sql.DB, targetSchema, sampleSchema, anchorTable string) error {
	dbx := sqlx.NewDb(db, "mysql")
	ancRows, err := dbx.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1;", targetSchema, anchorTable))
	if err != nil {
		return err
	}

	// we find tables we directly reference in the anchorTable via foreign keys
	fowardRels, err := fowardRelationships(ctx, db, anchorTable)
	if err != nil {
		return err
	}
	// we find other tables that reference the anchorTable via foreign keys
	reverseRels, err := reverseRelationships(ctx, db, anchorTable)
	if err != nil {
		return err
	}

	log.Printf("main reverse: %+v\n", reverseRels)

	for ancRows.Next() {
		ancRowData := make(map[string]interface{})
		ancRows.MapScan(ancRowData)

		stmts := []map[string][]interface{}{}
		stmt, vals, err := makeInsertQuery(ancRowData, db, sampleSchema, anchorTable)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if stmt != "" {
			insertQuery := map[string][]interface{}{stmt: vals}
			stmts = append([]map[string][]interface{}{insertQuery}, stmts...)
		}

		for i := 0; i < len(fowardRels); i++ {
			if d := ancRowData[fowardRels[i].tableCol]; d != nil {
				q := fmt.Sprintf("SELECT * FROM %s.%s WHERE `%s` = '%s';", targetSchema, fowardRels[i].referencedTable, fowardRels[i].referencedTableCol, d)
				r, err := dbx.QueryxContext(ctx, q)
				if err != nil {
					return err
				}
				for r.Next() {
					rd := make(map[string]interface{})
					r.MapScan(rd)
					stmt, vals, err := makeInsertQuery(rd, db, sampleSchema, fowardRels[i].referencedTable)
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
					if stmt != "" {
						insertQuery := map[string][]interface{}{stmt: vals}
						stmts = append([]map[string][]interface{}{insertQuery}, stmts...)
					}
				}
				trels, err := fowardRelationships(ctx, db, fowardRels[i].referencedTable)
				if err != nil {
					return err

				}
				fowardRels = append(fowardRels, trels...)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			for _, query := range stmts {
				for q, vals := range query {
					// log.Printf("insert %s\n", q)
					_, err := tx.ExecContext(ctx, q, vals...)
					if err != nil {
						return fmt.Errorf("tx rolled back %s: %w query { %s row %+s }", tx.Rollback(), err, q, vals)
					}
				}
			}
			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("could not commit tx: %w", err)
			}
		}

		for i := 0; i < len(reverseRels); i++ {
			err = getAllDeps(ctx, dbx, targetSchema, sampleSchema, reverseRels[i], ancRowData)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
