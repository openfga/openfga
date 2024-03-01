package openfgamobile

import (
	"database/sql"
	"log"
	"net/url"
	"os"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/run"
	"github.com/pressly/goose/v3"
)

func StartServer() {

	println("Starting server...")

	rootCmd := cmd.NewRootCommand()

	runCmd := run.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	os.Args = append(os.Args, "run",
		"--datastore-engine",
		"sqlite",
		"--datastore-uri", "file:test.db",
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}

	println("Server started!")
}

func MigrateDatabase() {
	println("Migrating database...")

	var uri, driver, dialect, migrationsPath string

	driver = "sqlite3"
	dialect = "sqlite3"
	migrationsPath = assets.SQLiteMigrationDir

	if uri == "" {
		uri = "file:test.db"
	}

	// Parse the database uri with the sqlite drivers function for it and update username/password, if set via flags
	dbURI, err := url.Parse(uri)

	if err != nil {
		log.Fatalf("invalid database uri: %v\n", err)
	}

	uri = dbURI.String()

	db, err := sql.Open(driver, uri)
	if err != nil {
		log.Fatalf("failed to open a connection to the datastore: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("failed to close the datastore: %v", err)
		}
	}()

	if err != nil {
		log.Fatalf("failed to initialize database connection: %v", err)
	}

	// TODO use goose.OpenDBWithDriver which already sets the dialect
	if err := goose.SetDialect(dialect); err != nil {
		log.Fatalf("failed to initialize the migrate command: %v", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("current version %d", currentVersion)

	if err := goose.Up(db, migrationsPath); err != nil {
		log.Fatal(err)
	}

	println("Database migrated!")
}
