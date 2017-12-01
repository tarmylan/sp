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
				Name: "brokers, b",
				// TODO fix me
				//Value: cli.NewStringSlice("172.16.10.222:9092,172.16.10.223:9092,172.16.10.224:9092"),
				Value: cli.NewStringSlice("172.16.10.222:9092"),
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
				Usage: "mysql table name",
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

	//config := sarama.NewConfig()
	//config.Producer.Return.Successes = false
	//config.Producer.Return.Errors = false
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
	if err != nil {
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

	for {
		select {
		case msg := <-tableConsumer.Messages():
			behavior := &Behavior{}
			if err := json.Unmarshal(msg.Value, behavior); err == nil {
				// pending
				pending = append(pending, behavior)
				offset = msg.Offset
			} else {
				log.Fatal(err)
			}
		case <-commitTicker.C:
			commit(lastTblName, consumerId, db, pending, offset)
			pending = pending[:0]
		}
	}
}

func commit(tblname, consumerId string, db *sql.DB, pending []*Behavior, offset int64) {
	if len(pending) == 0 {
		return
	}

	var err error
	var tx *sql.Tx
	var stmt *sql.Stmt

	tx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	doCommit := func(tx *sql.Tx) {
		if err := tx.Commit(); err != nil {
			// tx.Rollback()
			log.Fatal(err)
		}
	}

	defer func(tx *sql.Tx) {
		err := tx.Rollback()
		if err != sql.ErrTxDone && err != nil {
			log.Fatal(err)
		}
	}(tx)

	behavior_sql := "INSERT INTO behavior (char_id, name, action, data) VALUES (?, ?, ?, ?)"
	stmt, err = tx.Prepare(behavior_sql)
	if err != nil {
		log.Fatal(err)
	}

	handle_count := 0

	startSecs := time.Now().Unix()
	for _, value := range pending {
		if r, err := stmt.Exec(value.Id, value.Name, value.Action, string(value.Data)); err == nil {
		} else {
			log.Fatal(r, err)
		}

		var fields []Field
		err := json.Unmarshal(value.Data, &fields)
		if err != nil {
			log.Fatal("error:", err)
		}

		var values, copies []interface{}
		values = append(values, value.Id)
		values = append(values, value.Name)

		sqlStr := "INSERT INTO profile (id, name"
		valStr := "VALUES(?, ?"
		updStr := ""
		var count int
		for _, v := range fields {
			values = append(values, v.Value)
			copies = append(copies, v.Value)
			sqlStr += "," + v.Key
			valStr += ",?"
			if count > 0 {
				updStr += ","
			}
			updStr += v.Key + "=?"
			count++
		}
		sqlStr += ") " + valStr + ")"
		sqlStr += " ON DUPLICATE KEY UPDATE "
		sqlStr += updStr

		values = append(values, copies...)

		// update profile
		if r, err := tx.Exec(sqlStr, values...); err == nil {
		} else {
			log.Fatal(r, err)
		}

		handle_count += 2

		if handle_count > TRANSATION_HANDLE_UNIT {
			doCommit(tx)

			handle_count = 0
			tx, err = db.Begin()
			if err != nil {
				log.Fatal(err)
			}

			stmt, err = tx.Prepare(behavior_sql)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// write offset
	if r, err := tx.Exec("INSERT INTO kafka_offset (id, offset) VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=?", consumerId, offset, offset); err != nil {
		log.Fatal(r, err)
	}

	// commit the transation
	doCommit(tx)

	endSecs := time.Now().Unix()
	log.Infof("database table: %v, consumer id: %v, topic offset: %v, written: %v, cost:%vs", tblname, consumerId, offset, len(pending), endSecs-startSecs)
}
