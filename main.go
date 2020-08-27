package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	anchorParamsRE = regexp.MustCompile(`^(?P<table>\w+)(?:#(?P<column>\w+)=(?P<values>(?:\w+[,]?)+)*|$)`)
)

type sampleParams struct {
	rand   bool
	table  string
	column string
	data   []interface{}
}

func getAnchorTableWithParams(anchorTableFlagString string) sampleParams {
	matches := anchorParamsRE.MatchString(anchorTableFlagString)
	if !matches {
		flag.PrintDefaults()
		log.Fatalf("bad format for anchor table params")
	}
	res := anchorParamsRE.FindAllStringSubmatch(anchorTableFlagString, -1)
	data := make(map[string]string)
	sxp := anchorParamsRE.SubexpNames()
	for i, mm := range res[0] {
		if i == 0 {
			continue
		}
		data[sxp[i]] = mm
	}
	columnData := []interface{}{}
	for _, val := range strings.Split(data["values"], ",") {
		if val != "" {
			columnData = append(columnData, val)
		}
	}
	if len(columnData) <= 0 {
		return sampleParams{table: data["table"], rand: true}
	}
	return sampleParams{table: data["table"], column: data["column"], data: columnData}
}

// usage: sampledb -driver=dbdriver -host=dbhost -port=port -user=user -pass -targetschema=targetschema -sampleschema=sampleschema -anchor=table_name#col=val,val -nosample=tbl1,tbl2
func main() {
	driver := flag.String("driver", "mysql", "db driver")
	host := flag.String("host", "localhost", "db host")
	port := flag.String("port", "3306", "db port")
	user := flag.String("user", "root", "db user")
	pass := flag.String("pass", "root", "db user pass")
	targetSchema := flag.String("targetschema", "", "target schema name")
	sampleSchema := flag.String("sampleschema", "defaults to sample_db_{secs since January 1, 1970 UTC}", "sample schema name")
	anchorTable := flag.String("anchor", "",
		"table from which we'll start looking fot relationships, you can prepend a # followed by a list of comma separated ids after the table name to get specific "+
			"rows only, otherwise we'll randomly select 5 rows.\nfor example: \n\t-anchor=table#column=value,value,value&column=value,value,value")
	noSampleTable := flag.String("nosample", "", "comma separated list of tables name which will be copied in full")

	flag.Parse()

	// where we'll copy our sampled data
	var sampleSchemaName string
	if sampleSchema != nil {
		sampleSchemaName = *sampleSchema
	} else {
		sampleSchemaName = fmt.Sprintf("sample_db_%d", time.Now().Unix())

	}

	// parse anchor table params from the flag value
	sampleParams := getAnchorTableWithParams(*anchorTable)

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

	err = sample(context.TODO(), db, *targetSchema, sampleSchemaName, &sampleParams)
	if err != nil {
		log.Fatalf("could not sample db: %s", err)
	}
}

func connectDB(driver, host, port, user, pass string) (*sql.DB, error) {
	db, err := sql.Open(driver, fmt.Sprintf("%s:%s@tcp(%s:%s)/?multiStatements=true&max_execution_time=1000", user, pass, host, port))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Second)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	return db, db.Ping()
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

type foreignKeyConstraint struct {
	table              string
	tableCol           string
	referencedTable    string
	referencedTableCol string
}

type primaryKeyConstraint struct {
	table    string
	tableCol []string
}

// get table primary key constraint
func getTablePrimaryKeyConstraints(ctx context.Context, db *sql.DB, schema, table string) (*primaryKeyConstraint, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT DISTINCT(column_name) FROM information_schema.key_column_usage WHERE table_name = '%s' AND table_schema = '%s' AND constraint_name = 'PRIMARY';",
			table, schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := []string{}
	for rows.Next() {
		var colName string
		err = rows.Scan(&colName)
		if err != nil {
			return nil, err
		}
		cols = append(cols, colName)
	}
	return &primaryKeyConstraint{table: table, tableCol: cols}, nil
}

// returns columns and the tables that are referenced by the targetTable via FOREIGN KEY constraints
func fowardRelationships(ctx context.Context, db *sql.DB, schema, table string) ([]foreignKeyConstraint, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT column_name, referenced_column_name, referenced_table_name FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s' AND referenced_table_name != 'NULL';",
			schema, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]struct{}{}
	rels := []foreignKeyConstraint{}
	for rows.Next() {
		var colName, refColName, refTableName string
		err = rows.Scan(&colName, &refColName, &refTableName)
		if err != nil {
			return nil, err
		}
		if _, exists := cols[colName]; !exists {
			cols[colName] = struct{}{}
			rels = append(rels, foreignKeyConstraint{
				table: table, referencedTable: refTableName,
				tableCol: colName, referencedTableCol: refColName,
			})
		}
	}
	return rels, nil
}

// returns columns and the tables that reference the targetTable via FOREIGN KEY constraints
func reverseRelationships(ctx context.Context, db *sql.DB, schema, table string) ([]foreignKeyConstraint, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT table_name, column_name, referenced_column_name FROM information_schema.key_column_usage WHERE table_schema = '%s' AND referenced_table_name = '%s';",
			schema, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rels := []foreignKeyConstraint{}
	for rows.Next() {
		var colName, refColName, tableName string
		err = rows.Scan(&tableName, &colName, &refColName)
		if err != nil {
			return nil, err
		}
		rels = append(rels, foreignKeyConstraint{
			table: tableName, referencedTable: table,
			tableCol: colName, referencedTableCol: refColName,
		})
	}
	return rels, nil
}

func makeInsertQuery(refColName string, refColData interface{}, targetSchema, sampleSchema, table string) (string, error) {
	if refColData == nil {
		return "", sql.ErrNoRows
	}
	q := fmt.Sprintf("INSERT IGNORE INTO %s.%s SELECT * FROM %s.%s WHERE `%s` = '%s';", sampleSchema, table, targetSchema, table, refColName, refColData)
	return q, nil
}

func makeSampleQuery(targetSchema string, params *sampleParams) string {
	if params.rand {
		return fmt.Sprintf("SELECT * FROM %s.%s ORDER BY RAND() LIMIT 5;", targetSchema, params.table)
	}
	var whereClause string
	for i, param := range params.data {
		whereClause += fmt.Sprintf("`%s` = '%s'", params.column, param)
		if i < len(params.data)-1 {
			whereClause += " OR "
		}
	}
	return fmt.Sprintf("SELECT * FROM %s.%s WHERE %s;", targetSchema, params.table, whereClause)
}

type nodeVisitCache struct {
	columnName string
	data       interface{}
}

var (
	// key: table name, val: map[columnName]columnData
	fowardNodeVisit = make(map[string][]nodeVisitCache)
)

// inserts all rows referenced by this row via FOREIGN keys
func insertRowFowardRels(ctx context.Context, db *sqlx.DB, targetSchema, sampleSchema string, rels []foreignKeyConstraint, rowData map[string]interface{}) error {
	for _, rel := range rels {
		if columnData := rowData[rel.tableCol]; columnData != nil {
			dup := false
			tableCache := fowardNodeVisit[rel.referencedTable]
			for _, node := range tableCache {
				if node.columnName == rel.referencedTableCol && reflect.DeepEqual(node.data, columnData) {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			tableCache = append(tableCache, nodeVisitCache{rel.referencedTableCol, columnData})
			fowardNodeVisit[rel.referencedTable] = tableCache

			stmts := []string{}
			tblPk, err := getTablePrimaryKeyConstraints(ctx, db.DB, targetSchema, rel.referencedTable)
			if err != nil {
				return err
			}
			r, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s.%s WHERE `%s` = '%s';", targetSchema, rel.referencedTable, rel.referencedTableCol, columnData))
			if err != nil {
				return err
			}
			defer r.Close()
			datas := []map[string]interface{}{}
			for r.Next() {
				rd := make(map[string]interface{})
				r.MapScan(rd)
				datas = append(datas, rd)
			}

			moarFowRels, err := fowardRelationships(ctx, db.DB, targetSchema, rel.referencedTable)
			if err != nil {
				return err
			}
			for _, rd := range datas {
				if len(moarFowRels) > 0 {
					err = insertRowFowardRels(ctx, db, targetSchema, sampleSchema, moarFowRels, rd)
					if err != nil {
						return err
					}
				}
				stmt, err := makeInsertQuery(tblPk.tableCol[0], rd[tblPk.tableCol[0]], targetSchema, sampleSchema, tblPk.table)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				stmts = append([]string{stmt}, stmts...)
			}
			tx, err := db.Beginx()
			if err != nil {
				return err
			}
			for _, q := range stmts {
				log.Printf("insert %s\n", q)
				_, err := tx.ExecContext(ctx, q)
				if err != nil {
					return fmt.Errorf("tx failed: %s %w query %s", tx.Rollback(), err, q)
				}
			}
			err = tx.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func sample(ctx context.Context, db *sql.DB, targetSchema, sampleSchema string, params *sampleParams) error {
	dbx := sqlx.NewDb(db, "mysql")
	// we find tables we directly reference in the anchorTable via foreign keys
	fowardRels, err := fowardRelationships(ctx, db, targetSchema, params.table)
	if err != nil {
		return err
	}
	tablePkConstraint, err := getTablePrimaryKeyConstraints(ctx, db, sampleSchema, params.table)
	if err != nil {
		return err
	}
	// we'll get all the rows primary key column data and store it here
	pkData := [][]interface{}{}
	ancRows, err := dbx.QueryxContext(ctx, makeSampleQuery(targetSchema, params))
	if err != nil {
		return err
	}
	defer ancRows.Close()
	datas := []map[string]interface{}{}
	for ancRows.Next() {
		ancRowData := make(map[string]interface{})
		ancRows.MapScan(ancRowData)
		datas = append(datas, ancRowData)
	}
	for _, ancRowData := range datas {
		pd := []interface{}{}
		for _, t := range tablePkConstraint.tableCol {
			pd = append(pd, ancRowData[t])
		}
		pkData = append(pkData, pd)
		err = insertRowFowardRels(ctx, dbx, targetSchema, sampleSchema, fowardRels, ancRowData)
		if err != nil {
			return err
		}
		q, err := makeInsertQuery(tablePkConstraint.tableCol[0], ancRowData[tablePkConstraint.tableCol[0]], targetSchema, sampleSchema, params.table)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		log.Printf("insert %s\n", q)
		_, err = dbx.ExecContext(ctx, q)
		if err != nil {
			return fmt.Errorf("tx failed: %w query %s ", err, q)
		}
	}

	// we find other tables that reference the params.table via foreign keys
	reverseRels, err := reverseRelationships(ctx, db, targetSchema, params.table)
	if err != nil {
		return err
	}
	for _, rel := range reverseRels {
		var whereClause string
		var args []interface{}
		for idx, pkd := range pkData {
			for idxx, pkc := range pkd {
				whereClause += fmt.Sprintf("`%s` = '%s'", rel.tableCol, pkc)
				if idxx < len(pkd)-1 {
					whereClause += " OR "
				}
			}
			args = append(args, pkd[0])
			if idx < len(pkData)-1 {
				whereClause += " OR "
			}
		}
		if whereClause == "" {
			continue
		}
		err = sample(ctx, dbx.DB, targetSchema, sampleSchema, &sampleParams{table: rel.table, column: rel.tableCol, data: args})
		if err != nil {
			return err
		}
		q := fmt.Sprintf("INSERT IGNORE INTO %s.%s SELECT * FROM %s.%s WHERE %s;", sampleSchema, rel.table, targetSchema, rel.table, whereClause)
		log.Printf("insert %s\n", q)
		_, err = dbx.ExecContext(ctx, q)
		if err != nil {
			return err
		}
	}

	return nil
}
