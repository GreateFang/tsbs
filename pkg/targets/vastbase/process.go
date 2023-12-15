package vastbase

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "gitee.com/opengauss/openGauss-connector-go-pq"
	pq "gitee.com/opengauss/openGauss-connector-go-pq"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/tsbs/pkg/targets"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

const (
	//vastbase not support returning clause
	//insertTagsSQL = `INSERT INTO tags(%s) VALUES %s ON CONFLICT DO NOTHING RETURNING *`
	insertTagsSQL = `INSERT INTO tags(%s) VALUES %s ON CONFLICT (%s) DO NOTHING`
	getTagsSQL    = `SELECT * FROM tags`
	numExtraCols  = 2 // one for json, one for tags_id
)

type processor struct {
	_db      *sql.DB
	_csi     *syncCSI
	_pgxConn *pgx.Conn
	opts     *LoadingOptions
	driver   string
	dbName   string
}

func newProcessor(opts *LoadingOptions, driver, dbName string) *processor {
	return &processor{
		opts:   opts,
		driver: driver,
		dbName: dbName,
	}
}

type syncCSI struct {
	m     map[string]int64
	mutex *sync.RWMutex
}

type insertData struct {
	tags   string
	fields string
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

func (p *processor) Init(_ int, doLoad, hashWorkers bool) {
	if !doLoad {
		return
	}
	p._db = MustConnect(p.driver, p.opts.GetConnectString(p.dbName))
	if hashWorkers {
		p._csi = newSyncCSI()
	} else {
		p._csi = globalSyncCSI
	}
	if !p.opts.ForceTextFormat {
		conn, err := stdlib.AcquireConn(p._db)
		if err != nil {
			panic(err)
		}
		p._pgxConn = conn
	}
	//p.loadExistingTagsInCache(p._db)
}

//func (p *processor) loadExistingTagsInCache(db *sql.DB) {
//	p._csi.mutex.Lock()
//	res, err := db.Query(getTagsSQL)
//	if err != nil {
//		panic(err)
//	}
//	tagCols := tableCols[tagsKey]
//	ret := p.sqlTagsToCacheLine(res, err, tagCols)
//	for k, v := range ret {
//		p._csi.m[k] = v
//	}
//	p._csi.mutex.Unlock()
//}

func (p *processor) sqlTagsToCacheLine(res *sql.Rows, err error, tagCols []string) map[string]int64 {
	// Results will be used to make a Golang index for faster inserts
	resCols, _ := res.Columns()
	resVals := make([]interface{}, len(resCols))
	resValsPtrs := make([]interface{}, len(resCols))
	for i := range resVals {
		resValsPtrs[i] = &resVals[i]
	}
	ret := make(map[string]int64)
	for res.Next() {
		err = res.Scan(resValsPtrs...)
		if err != nil {
			panic(err)
		}

		var key string
		if p.opts.UseJSON {
			decodedTagset := map[string]string{}
			json.Unmarshal(resVals[1].([]byte), &decodedTagset)
			key = decodedTagset[tagCols[0]]
		} else {
			key = fmt.Sprintf("%v", resVals[1])
		}
		ret[key] = resVals[0].(int64)
	}
	res.Close()
	return ret
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p._db.Close()
	}
	if p._pgxConn != nil {
		err := stdlib.ReleaseConn(p._db, p._pgxConn)
		if err != nil {
			panic(err)
		}
	}
}

// 对给定的数据批次进行处理，包括统计行数和度量计数，并根据需要加载数据和记录日志
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*hypertableArr) //将 b 转换为 *hypertableArr 类型
	rowCnt := 0
	metricCnt := uint64(0)
	for hypertable, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(hypertable, rows)

			if p.opts.LogBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0
	return metricCnt, uint64(rowCnt)
}

func (p *processor) processCSI(hypertable string, rows []*insertData) uint64 {
	colLen := len(tableCols[hypertable]) + len(tableCols[tagsKey]) + 1
	if p.opts.InTableTag {
		colLen++
	}
	_, dataRows, numMetrics := p.splitTagsAndMetrics(rows, colLen)

	cols := make([]string, 0, colLen)
	//cols = append(cols, "time", "tags_id", "additional_tags")
	cols = append(cols, "time")
	cols = append(cols, tableCols[tagsKey]...)
	//if p.opts.InTableTag {
	//	cols = append(cols, tableCols[tagsKey]...)
	//}
	cols = append(cols, tableCols[hypertable]...)

	if p.opts.ForceTextFormat {
		panic("no implement")
		tx := MustBegin(p._db)
		stmt, err := tx.Prepare(pq.CopyIn(hypertable, cols...))
		if err != nil {
			panic(err)
		}

		//for _, r := range dataRows {
		//	//stmt.Exec(r...)
		//}
		_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	} else {
		if !p.opts.UseInsert {
			//panic("no implement")
			rows := pgx.CopyFromRows(dataRows)
			inserted, err := p._pgxConn.CopyFrom(context.Background(), pgx.Identifier{hypertable}, cols, rows)

			if err != nil {
				fmt.Println("rows ", rows)
				panic(err)
			}

			if inserted != int64(len(dataRows)) {
				fmt.Fprintf(os.Stderr, "Failed to insert all the data! Expected: %d, Got: %d", len(dataRows), inserted)
				os.Exit(1)
			}
		} else {
			tx := MustBegin(p._db)
			var stmt *sql.Stmt
			var err error

			stmtString := genBatchInsertStmt(hypertable, cols, len(dataRows))
			stmt, err = tx.Prepare(stmtString)

			_, err = stmt.Exec(flatten(dataRows)...)
			if err != nil {
				panic(err)
			}

			err = stmt.Close()
			if err != nil {
				panic(err)
			}

			err = tx.Commit()
			if err != nil {
				panic(err)
			}
		}
	}

	return numMetrics
}

func (p *processor) splitTagsAndMetrics(rows []*insertData, dataCols int) ([][]string, [][]interface{}, uint64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	numMetrics := uint64(0)
	commonTagsLen := len(tableCols[tagsKey])

	for _, data := range rows {
		// Split the tags into individual common tags and an extra bit leftover
		// for non-common tags that need to be added separately. For each of
		// the common tags, remove everything before = in the form <label>=<val>
		// since we won't need it.
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		//var json interface{}
		//if len(tags) > commonTagsLen {
		//	json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		//}

		metrics := strings.Split(data.fields, ",")
		numMetrics += uint64(len(metrics) - 1) // 1 field is timestamp

		timeInt, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		ts := time.Unix(0, timeInt)

		// use nil at 2nd position as placeholder for tagKey
		r := make([]interface{}, 1, dataCols)
		r[0] = ts
		if !p.opts.UseInsert {
			r[0] = ts
			//pgtype.Timestamptz{Time: ts, Status: pgtype.Null}
		}
		//if p.opts.InTableTag {
		//	r = append(r, tags[0])
		//}
		//tagDatarow := convertValsToSQLBasedOnType(tags[:commonTagsLen], p.opts.TagColumnTypes[:commonTagsLen])
		//for _, v := range tagDatarow {
		//	if v == "NULL" || v == "" {
		//		r = append(r, nil)
		//		continue
		//	}
		//	r = append(r, v)
		//}
		//fmt.Println("tags:", tagDatarow)
		for i, v := range tags {
			if v == "" {
				r = append(r, nil)
				continue
			}
			switch p.opts.TagColumnTypes[i] {
			case "string":
				r = append(r, v)
			default:
				num, err := strconv.ParseFloat(v, 64)
				if err != nil {
					panic(err)
				}
				r = append(r, num)
			}

			//r = append(r, v)
		}

		for _, v := range metrics[1:] {
			if v == "" {
				r = append(r, nil)
				continue
			}

			num, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}

			r = append(r, num)
		}

		dataRows = append(dataRows, r)
		//tagRows = append(tagRows, tags[:commonTagsLen])
	}
	//fmt.Println("datarows[0]", dataRows[0])

	return tagRows, dataRows, numMetrics
}

func (p *processor) insertTags(db *sql.DB, tagRows [][]string) map[string]int64 {
	tagCols := tableCols[tagsKey]
	cols := tagCols
	values := make([]string, 0)
	commonTagsLen := len(tagCols)
	if p.opts.UseJSON {
		cols = []string{"tagset"}
		for _, row := range tagRows {
			jsonValues := convertValsToJSONBasedOnType(row[:commonTagsLen], p.opts.TagColumnTypes[:commonTagsLen])
			jsonX := "('{"
			for i, k := range tagCols {
				if i != 0 {
					jsonX += ","
				}
				jsonX += fmt.Sprintf("\"%s\":%s", k, jsonValues[i])
			}
			jsonX += "}')"
			values = append(values, jsonX)
		}
	} else {
		for _, val := range tagRows {
			sqlValues := convertValsToSQLBasedOnType(val[:commonTagsLen], p.opts.TagColumnTypes[:commonTagsLen])
			row := fmt.Sprintf("(%s)", strings.Join(sqlValues, ","))
			values = append(values, row)
		}
	}
	tx := MustBegin(db)
	defer tx.Commit()
	// INSERT INTO tags(%s) VALUES %s ON CONFLICT DO NOTHING RETURNING *
	res, err := tx.Query(fmt.Sprintf(insertTagsSQL, strings.Join(cols, ","), strings.Join(values, ","), strings.Join(cols, ",")))
	if err != nil {
		panic(err)
	}

	ret := p.sqlTagsToCacheLine(res, err, tagCols)
	return ret
}

func genBatchInsertStmt(hypertable string, cols []string, rows int) string {
	colLen := len(cols)
	if rows*colLen > math.MaxUint16 {
		panic(fmt.Errorf("Max allowed batch size when not using COPY is %d due to PosgreSQL limitation of max parameters", math.MaxUint16/colLen))
	}
	insertStmt := bytes.NewBufferString(fmt.Sprintf("INSERT INTO %s(%s) VALUES", hypertable, strings.Join(cols, ",")))
	rowPrefix := ""
	for i := 0; i < rows; i++ {
		insertStmt.WriteString(rowPrefix + " (")
		rowPrefix = ","
		colPrefix := ""
		for j := range cols {
			insertStmt.WriteString(fmt.Sprintf("%s$%d", colPrefix, (i*colLen)+(j+1)))
			colPrefix = ","
		}
		insertStmt.WriteString(")")
	}
	return insertStmt.String()
}

func flatten(dataRows [][]interface{}) []interface{} {
	flattened := make([]interface{}, len(dataRows)*len(dataRows[0]))
	for i, row := range dataRows {
		cols := len(row)
		for j := range row {
			flattened[i*cols+j] = row[j]
		}
	}
	return flattened
}

func subsystemTagsToJSON(tags []string) map[string]interface{} {
	jsonToReturn := map[string]interface{}{}
	for _, t := range tags {
		args := strings.Split(t, "=")
		jsonToReturn[args[0]] = args[1]
	}
	return jsonToReturn
}

func convertValsToJSONBasedOnType(values []string, types []string) []string {
	return convertValsToBasedOnType(values, types, `"`, "null")
}

func convertValsToSQLBasedOnType(values []string, types []string) []string {
	return convertValsToBasedOnType(values, types, "'", "NULL")
}

func convertValsToBasedOnType(values []string, types []string, quotemark string, null string) []string {
	sqlVals := make([]string, len(values))
	for i, val := range values {
		if val == "" {
			sqlVals[i] = null
			continue
		}
		switch types[i] {
		case "string":
			sqlVals[i] = quotemark + val + quotemark
		default:
			sqlVals[i] = val
		}
	}

	return sqlVals
}
