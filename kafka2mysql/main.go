package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	cli "gopkg.in/urfave/cli.v2"
)

const (
	TRANSATION_HANDLE_UNIT = 2000
)

type Behavior struct {
	Key     string          `json:"key"`
	TraceAt int64           `json:"trace_at"`
	Id      uint32          `json:"id"`
	Name    string          `json:"name"`
	Action  int32           `json:"action"`
	Data    json.RawMessage `json:"data"`
}

type Field struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func main() {
	app := &cli.App{
		Name:    "kafka2mysql",
		Usage:   `Store Kafka Topic To MySQL Table`,
		Version: "0.1",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("172.16.10.222:9092", "172.16.10.223:9092", "172.16.10.224:9092"),
				Usage: "kafka brokers address",
			},
			&cli.StringFlag{
				Name:  "table-topic",
				Value: "trace-MYGAME",
				Usage: "topic name that contains the table",
			},
			&cli.StringFlag{
				Name:  "table",
				Value: "profile",
				Usage: "table name in behavior to archive",
			},
			&cli.StringFlag{
				Name:  "url",
				Value: "ubuntu:123456@tcp(172.16.10.212)/game_monitor",
				Usage: "mysql url",
			},
			&cli.StringFlag{
				Name:  "trace-tblname",
				Value: "behavior",
				Usage: "mysql table name, aware of timeformat in golang",
			},
			&cli.DurationFlag{
				Name:  "commit-interval",
				Value: time.Second,
				Usage: "interval for committing pending data to psql",
			},
		},
		Action: processor,
	}
	app.Run(os.Args)
}

func processor(c *cli.Context) error {
	brokers := c.StringSlice("brokers")
	table_topic := c.String("table-topic")
	table := c.String("table")
	url := c.String("url")
	trace_tblname := c.String("trace-tblname")
	commit_interval := c.Duration("commit-interval")

	log.Info("brokers:", brokers)
	log.Info("table-topic:", table_topic)
	log.Info("table:", table)
	log.Info("url:", url)
	log.Info("trace-tblname:", trace_tblname)
	log.Info("commit-interval:", commit_interval)

	// unique consumer name to store in mysql
	consumerId := fmt.Sprintf("%v-%v-%v", table_topic, table, trace_tblname)
	log.Info("consumerId:", consumerId)

	// connect to mysql
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// read offset
	offset := sarama.OffsetOldest
	err = db.QueryRow("SELECT offset FROM kafka_offset WHERE id = ? LIMIT 1", consumerId).Scan(&offset)
	if err != nil && err != sql.ErrNoRows {
		log.Fatal(err)
	}

	log.Printf("consuming from topic:%v offset:%v", table_topic, offset)

	tableConsumer, err := consumer.ConsumePartition(table_topic, 0, offset)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := tableConsumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	lastTblName := trace_tblname
	log.Info("started")

	commitTicker := time.NewTicker(commit_interval)

	var pending []*Behavior
	var pending_count int
	for {
		select {
		case msg := <-tableConsumer.Messages():
			behavior := &Behavior{}
			if err := json.Unmarshal(msg.Value, behavior); err == nil {
				// pending
				pending_count++
				pending = append(pending, behavior)
				offset = msg.Offset
			} else {
				log.Fatal(err)
			}

			//
			if pending_count >= TRANSATION_HANDLE_UNIT {
				err := commit(lastTblName, consumerId, db, pending, offset)
				pending_count = 0
				pending = pending[:0]
				if err != nil {
					log.Fatal(err)
				}
			}
		case <-commitTicker.C:
			err := commit(lastTblName, consumerId, db, pending, offset)
			pending_count = 0
			pending = pending[:0]
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func commit(tblname, consumerId string, db *sql.DB, pending []*Behavior, offset int64) error {
	if len(pending) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("tx begin error %v", err)
	}

	defer func(tx *sql.Tx) {
		err := tx.Rollback()
		if err != sql.ErrTxDone && err != nil {
			log.Error(err)
			return
		}
	}(tx)

	/*
		behavior_sql := "INSERT INTO behavior (char_id, name, action, data) VALUES (?, ?, ?, ?)"
		stmt, err := tx.Prepare(behavior_sql)
		if err != nil {
			return fmt.Errorf("prepare behavior sql stmt error %v", err)
		}
	*/

	startTime := time.Now()
	for _, v := range pending {
		// behavior records
		/*
			if _, err := stmt.Exec(v.Id, v.Name, v.Action, string(v.Data)); err != nil {
				return fmt.Errorf("behavior record %v, %v, %v error %v", v.Id, v.Name, v.Action, err)
			}
		*/
		insertSql, updateSql, valuesSql, values, err := convertFields(v)
		if err != nil {
			return fmt.Errorf("convertFields Error %v", err)
		}

		// behavior insert
		sqlStr, fieldValues := behavior2Sql(v, insertSql, updateSql, valuesSql, values)
		if _, err := tx.Exec(sqlStr, fieldValues...); err != nil {
			return fmt.Errorf("insert behavior %v, %v, %v error %v", v.Id, v.Name, v.Action, err)
		}

		// profile update
		sqlStr, fieldValues = behavior2ProfileSql(v, insertSql, updateSql, valuesSql, values)
		if _, err := tx.Exec(sqlStr, fieldValues...); err != nil {
			return fmt.Errorf("update profile %v, %v, %v error %v", v.Id, v.Name, v.Action, err)
		}
	}

	// write offset
	if _, err := tx.Exec("INSERT INTO kafka_offset (id, offset) VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=?",
		consumerId, offset, offset); err != nil {
		return fmt.Errorf("update kafka_offset %v, %v error %v", consumerId, offset, err)
	}

	// commit the transation
	if err := tx.Commit(); err != nil {
		// tx.Rollback()
		return fmt.Errorf("mysql commit error %v", err)
	}

	elapsed := time.Now().Sub(startTime)
	log.Infof("database table: %v, consumer id: %v, topic offset: %v, written: %v, cost:%v", tblname, consumerId, offset, len(pending), elapsed)
	return nil
}

func behavior2ProfileSql(v *Behavior, insertSql, updateSql, valuesSql string, values []interface{}) (sqlStr string, fieldValues []interface{}) {
	sqlStr = "INSERT INTO profile (id, name"
	sqlStr += insertSql

	valStr := "VALUES(?, ?"
	valStr += valuesSql
	sqlStr += ") " + valStr + ")"

	// update
	sqlStr += " ON DUPLICATE KEY UPDATE "
	sqlStr += updateSql

	fieldValues = []interface{}{v.Id, v.Name}
	fieldValues = append(fieldValues, values...)

	// update
	fieldValues = append(fieldValues, values...)
	return
}

func behavior2Sql(v *Behavior, insertSql, updateSql, valuesSql string, values []interface{}) (sqlStr string, fieldValues []interface{}) {
	sqlStr = "INSERT INTO behavior (char_id, name, action, trace_at"
	sqlStr += insertSql

	valStr := "VALUES(?, ?, ?, ?"
	valStr += valuesSql
	sqlStr += ") " + valStr + ")"

	fieldValues = []interface{}{v.Id, v.Name, v.Action, v.TraceAt}
	fieldValues = append(fieldValues, values...)
	return
}

func convertFields(v *Behavior) (insertSql, updateSql, valuesSql string, values []interface{}, err error) {
	var fields []Field
	err = json.Unmarshal(v.Data, &fields)
	if err != nil {
		return
	}

	var count int
	for _, v := range fields {
		values = append(values, v.Value)
		insertSql += "," + v.Key
		valuesSql += ",?"
		if count > 0 {
			updateSql += ","
		}
		updateSql += v.Key + "=?"
		count++
	}
	return
}
