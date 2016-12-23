package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/gabs"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
	cli "gopkg.in/urfave/cli.v2"
)

const (
	timeFormat = "2006-01-02T15:04"
	bucketName = "snapshot"
	offsetKey  = "__offset__"
)

func main() {
	app := &cli.App{
		Name:    "archiver",
		Usage:   "Create Commit Log Snapshots from Kafka to BoltDB",
		Version: "0.1",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "brokers, b",
				Value: cli.NewStringSlice("localhost:9092"),
				Usage: "kafka brokers address",
			},
			&cli.StringFlag{
				Name:  "topic, t",
				Value: "commitlog",
				Usage: "topic name for consuming commit log",
			},
			&cli.StringFlag{
				Name:  "base",
				Value: "./snapshot.db",
				Usage: "base snapshot path, created if file doesn't exists",
			},
			&cli.StringFlag{
				Name:  "snapshot",
				Value: "./snapshot-20060102.db",
				Usage: "snapshot path, aware of timeformat in golang",
			},
			&cli.StringFlag{
				Name:  "primarykey,PK",
				Value: "",
				Usage: "use json field as primary key instead of message key, format: https://github.com/Jeffail/gabs",
			},
			&cli.DurationFlag{
				Name:  "rotate",
				Value: 4 * time.Hour,
				Usage: "backup rotate duration",
			},
			&cli.DurationFlag{
				Name:  "commit-interval",
				Value: 30 * time.Second,
				Usage: "longest commit interval to BoltDB",
			},
		},
		Action: processor,
	}
	app.Run(os.Args)
}

func processor(c *cli.Context) error {
	log.Println("brokers:", c.StringSlice("brokers"))
	log.Println("topic:", c.String("topic"))
	log.Println("base:", c.String("base"))
	log.Println("snapshot:", c.String("snapshot"))
	log.Println("primarykey:", c.String("primarykey"))
	log.Println("rotate:", c.Duration("rotate"))
	log.Println("commit-interval:", c.Duration("commit-interval"))

	db, err := bolt.Open(c.String("base"), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	consumer, err := sarama.NewConsumer(c.StringSlice("brokers"), nil)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// read offset
	offset := sarama.OffsetOldest
	db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(bucketName)); b != nil {
			if v := b.Get([]byte(offsetKey)); v != nil {
				offset = int64(binary.LittleEndian.Uint64(v))
			}
			log.Printf("%+v\n", b.Stats())
		}
		return nil
	})
	log.Println("consuming from offset:", offset)

	partitionConsumer, err := consumer.ConsumePartition(c.String("topic"), 0, offset)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	pending := make(map[string][]byte)
	rotateTicker := time.NewTicker(c.Duration("rotate"))
	commitTicker := time.NewTicker(c.Duration("commit-interval"))

	log.Println("started")
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			pending[string(msg.Key)] = msg.Value
			offset = msg.Offset
		case <-commitTicker.C:
			commit(c.String("primarykey"), db, pending, offset)
			pending = make(map[string][]byte)
		case <-rotateTicker.C:
			if err := db.View(func(tx *bolt.Tx) error {
				newfile := time.Now().Format(c.String("snapshot"))
				log.Println("new archive:", newfile)
				return tx.CopyFile(newfile, 0666)
			}); err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func commit(primkey string, db *bolt.DB, pending map[string][]byte, offset int64) {
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		// write messages
		for key, value := range pending {
			if primkey != "" { // json field as primary key
				if jsonParsed, err := gabs.ParseJSON(value); err == nil {
					key := fmt.Sprint(jsonParsed.Path(primkey).Data())
					if err = bucket.Put([]byte(key), value); err != nil {
						log.Println(err)
					}
				} else {
					log.Println(err)
				}
			} else { // message key as primary key
				if err = bucket.Put([]byte(key), value); err != nil {
					log.Println(err)
				}
			}
		}

		// write offset
		offset_encode := make([]byte, 8)
		binary.LittleEndian.PutUint64(offset_encode, uint64(offset))
		if err = bucket.Put([]byte(offsetKey), offset_encode); err != nil {
			return err
		}
		log.Println("written:", len(pending), "offset:", offset)
		return nil
	}); err != nil {
		log.Fatalln(err)
	}
}
