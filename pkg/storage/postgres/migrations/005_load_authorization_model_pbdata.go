package migrations

import (
	"context"
	"database/sql"
	"log"

	"github.com/jackc/pgx/v5/pgconn"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/proto"
)

func getStores(ctx context.Context, tx *sql.Tx) ([]string, error) {
	var storeIDs []string
	var storeID string

	rows, err := tx.QueryContext(ctx, "SELECT id FROM store")
	if err != nil {
		return storeIDs, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&storeID)
		if err != nil {
			return storeIDs, err
		}

		storeIDs = append(storeIDs, storeID)
	}

	return storeIDs, nil
}

func getAuthorizationModelIDs(ctx context.Context, tx *sql.Tx, storeID string) ([]string, error) {
	var modelIDs []string
	var modelID string

	listStmt := `
	SELECT DISTINCT(authorization_model_id)
	FROM authorization_model
	WHERE store = $1 ORDER BY authorization_model_id desc;`

	rows, err := tx.QueryContext(ctx, listStmt, storeID)
	if err != nil {
		return modelIDs, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&modelID)
		if err != nil {
			return modelIDs, err
		}

		modelIDs = append(modelIDs, modelID)
	}

	return modelIDs, nil
}

func converge(ctx context.Context, tx *sql.Tx, storeID, modelID string) error {
	listStmt := `
		SELECT schema_version, type, type_definition, pbdata
		FROM authorization_model
		WHERE store = $1 AND authorization_model_id = $2 AND pbdata IS NULL;`

	rows, err := tx.QueryContext(ctx, listStmt, storeID, modelID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var schemaVersion string
	var typeDefs []*openfgav1.TypeDefinition

	for rows.Next() {
		var typeName string
		var marshalledTypeDef []byte
		var marshalledModel []byte
		err = rows.Scan(&schemaVersion, &typeName, &marshalledTypeDef, &marshalledModel)
		if err != nil {
			return err
		}

		log.Println("===>", schemaVersion, typeName)

		var typeDef openfgav1.TypeDefinition
		if err := proto.Unmarshal(marshalledTypeDef, &typeDef); err != nil {
			return err
		}

		typeDefs = append(typeDefs, &typeDef)
	}

	if err = rows.Err(); err != nil {
		return err
	}

	model := &openfgav1.AuthorizationModel{
		SchemaVersion:   schemaVersion,
		Id:              modelID,
		TypeDefinitions: typeDefs,
	}

	pbdata, err := proto.Marshal(model)
	if err != nil {
		return err
	}

	insertStmt := `
	INSERT INTO authorization_model (store, authorization_model_id, schema_version, type, type_definition, pbdata)
	VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT ON CONSTRAINT authorization_model_pkey DO NOTHING;`

	_, err = tx.ExecContext(ctx, insertStmt, storeID, modelID, schemaVersion, "", nil, pbdata)
	if err != nil {
		// gracefully handle error if converged row already exists
		if pe, ok := err.(*pgconn.PgError); ok && pe.Code != "23505" {
			return err
		}

	}

	deleteStmt := `
	DELETE FROM authorization_model
	WHERE store = $1 AND authorization_model_id = $2 AND pbdata IS NULL;`

	_, err = tx.ExecContext(ctx, deleteStmt, storeID, modelID)
	if err != nil {
		return err
	}

	return nil
}

func up005(ctx context.Context, tx *sql.Tx) error {
	// Get all stores
	storeIDs, err := getStores(ctx, tx)
	if err != nil {
		return err
	}

	for _, storeID := range storeIDs {
		log.Println("=> storeid", storeID)

		// Get all models for each store
		modelIDs, err := getAuthorizationModelIDs(ctx, tx, storeID)
		if err != nil {
			return err
		}

		// For each model, load all rows, converge+insert, cleanup
		for _, modelID := range modelIDs {
			log.Println("=> modelid", storeID)
			err = converge(ctx, tx, storeID, modelID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func down005(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func init() {
	migrations["005_load_authorization_model_pbdata"] = migration{up005, down005}
}
