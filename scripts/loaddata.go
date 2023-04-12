// Use: inside openfga folder:
// make start-postgres && make migrate-postgres && go run /loaddata.go

package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"
)

type TupleV1 struct {
	Store      string
	ObjectType string
	ObjectID   string
	Relation   string
	User       string
	UserType   string
	ULID       string
	InsertedAt string
}

type TupleChangeV1 struct {
	Store      string
	ObjectType string
	ObjectID   string
	Relation   string
	User       string
	Operation  int
	ULID       string
	InsertedAt string
}

const (
	layout = "2006-01-02 15:04:05.000000-07:00"
	Write  = 0
	Delete = 1
)

func main() {
	argEngine := os.Args[1]
	argConnectionString := os.Args[2]
	argTotalTuples, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Panic(err)
	}

	var driver string
	switch argEngine {
	case "postgres":
		driver = "pgx"
	case "mysql":
		driver = "mysql"
	default:
		log.Panic("unknown database")
	}

	db, err := sql.Open(driver, argConnectionString)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	storeID := ulid.Make().String()

	if err := insertStore(argEngine, db, storeID); err != nil {
		log.Panic(err)
	}

	tuples, changes := generateTuplesAndChanges(storeID, argTotalTuples)

	if err := write(db, argEngine, "tuples.csv", "changelog.csv", tuples, changes); err != nil {
		log.Panic(err)
	}
}

func generateTuplesAndChanges(storeID string, totalTuples int) ([]TupleV1, []TupleChangeV1) {
	tuples := make([]TupleV1, 0, totalTuples+1)
	changes := make([]TupleChangeV1, 0, totalTuples+1)

	t := TupleV1{
		Store:      storeID,
		ObjectType: "document",
		ObjectID:   "budget",
		Relation:   "viewer",
		User:       "user:*",
		UserType:   "userset",
		ULID:       ulid.Make().String(),
		InsertedAt: time.Now().Format(layout),
	}
	tuples = append(tuples, t)
	changes = append(changes, TupleChangeV1{
		Store:      storeID,
		ObjectType: t.ObjectType,
		ObjectID:   t.ObjectID,
		Relation:   t.Relation,
		User:       t.User,
		ULID:       t.ULID,
		InsertedAt: t.InsertedAt,
		Operation:  Write,
	})

	for i := 0; i < totalTuples/2; i++ {
		t := TupleV1{
			Store:      storeID,
			ObjectType: "document",
			ObjectID:   "budget",
			Relation:   "viewer",
			User:       fmt.Sprintf("user:%d", i),
			UserType:   "user",
			ULID:       ulid.Make().String(),
			InsertedAt: time.Now().Format(layout),
		}
		tuples = append(tuples, t)
		changes = append(changes, TupleChangeV1{
			Store:      storeID,
			ObjectType: t.ObjectType,
			ObjectID:   t.ObjectID,
			Relation:   t.Relation,
			User:       t.User,
			ULID:       t.ULID,
			InsertedAt: t.InsertedAt,
			Operation:  Write,
		})

		t = TupleV1{
			Store:      storeID,
			ObjectType: "document",
			ObjectID:   "budget",
			Relation:   "viewer",
			User:       fmt.Sprintf("group:%d#member", i),
			UserType:   "userset",
			ULID:       ulid.Make().String(),
			InsertedAt: time.Now().Format(layout),
		}
		tuples = append(tuples, t)
		changes = append(changes, TupleChangeV1{
			Store:      storeID,
			ObjectType: t.ObjectType,
			ObjectID:   t.ObjectID,
			Relation:   t.Relation,
			User:       t.User,
			ULID:       t.ULID,
			InsertedAt: t.InsertedAt,
			Operation:  Write,
		})
	}

	return tuples, changes
}

func write(db *sql.DB, engine, tuplesCsv, changesCsv string, tuples []TupleV1, changes []TupleChangeV1) error {
	g, _ := errgroup.WithContext(context.Background())

	g.SetLimit(2)

	g.Go(func() error {
		err := writeTuplesToCSV(tuplesCsv, tuples)
		if err != nil {
			return err
		}

		return copyFromFileToTable(engine, "tuple", tuplesCsv, db)
	})

	g.Go(func() error {
		err := writeChangesToCSV(changesCsv, changes)
		if err != nil {
			return err
		}

		return copyFromFileToTable(engine, "changelog", changesCsv, db)
	})

	return g.Wait()
}

func writeChangesToCSV(changesCsv string, changes []TupleChangeV1) error {
	defer timeTrack(time.Now(), "writeChangesToCSV")
	log.Printf("writing changes to CSV")
	file, err := os.OpenFile(changesCsv, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.Write([]string{"store", "object_type", "object_id", "relation", "_user", "operation", "ulid", "inserted_at"})
	if err != nil {
		return err
	}

	for _, t := range changes {
		err = writer.Write([]string{t.Store, t.ObjectType, t.ObjectID, t.Relation, t.User, strconv.Itoa(t.Operation), t.ULID, t.InsertedAt})
		if err != nil {
			return err
		}
	}

	writer.Flush()

	err = writer.Error()
	if err != nil {
		return err
	}
	return nil
}

func writeTuplesToCSV(tuplesCsv string, tuples []TupleV1) error {
	defer timeTrack(time.Now(), "writeTuplesToCSV")
	log.Printf("writing tuples to CSV")
	file, err := os.OpenFile(tuplesCsv, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.Write([]string{"store", "object_type", "object_id", "relation", "_user", "user_type", "ulid", "inserted_at"})
	if err != nil {
		return err
	}

	for _, t := range tuples {
		err = writer.Write([]string{t.Store, t.ObjectType, t.ObjectID, t.Relation, t.User, t.UserType, t.ULID, t.InsertedAt})
		if err != nil {
			return err
		}
	}

	writer.Flush()

	err = writer.Error()
	if err != nil {
		return err
	}
	return nil
}

func copyFromFileToTable(engine, tableName, filename string, db *sql.DB) error {
	if err := copyCsvToContainer(engine, filename); err != nil {
		return err
	}
	defer timeTrack(time.Now(), fmt.Sprintf("copyFromFileToTable %s", tableName))

	var res sql.Result
	var err error
	if engine == "postgres" {
		res, err = db.Exec(fmt.Sprintf("COPY %s FROM '%s' WITH CSV HEADER", tableName, "/tmp/"+filename))
	} else {
		res, err = db.Exec(fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' IGNORE 1 LINES", "/tmp/"+filename, tableName))
	}
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()

	log.Printf("wrote %d rows to table %s", rows, tableName)
	return nil
}

func copyCsvToContainer(engine, filename string) error {
	defer timeTrack(time.Now(), fmt.Sprintf("copyCsvToContainer %s", filename))

	return exec.Command("docker", "cp", filename, fmt.Sprintf("%s:/tmp/%s", engine, filename)).Run()
}

func insertStore(engine string, db *sql.DB, storeID string) error {
	defer timeTrack(time.Now(), "insertStore")

	stbl := sq.StatementBuilder
	var timeStamp any
	if engine == "postgres" {
		timeStamp = "NOW()"
		stbl = stbl.PlaceholderFormat(sq.Dollar)
	} else if engine == "mysql" {
		timeStamp = sq.Expr("NOW()")
	}

	_, err := stbl.RunWith(db).
		Insert("store").
		Columns("id", "name", "created_at", "updated_at").
		Values(storeID, storeID, timeStamp, timeStamp).
		Exec()
	return err
}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
