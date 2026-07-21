package sqlite

import (
	"context"
	"testing"
)

// TestOpenInMemoryRoundTrip exercises the full path against a real ":memory:" database:
// Open mints an in-memory database, a tuple row is seeded into the standard schema, and a
// query built through the sqlite dialect reads it back. This confirms the rendered SQL
// (split subject columns, "?" placeholders) actually runs on SQLite and that the pinned
// single connection keeps the seeded data visible to the query.
func TestOpenInMemoryRoundTrip(t *testing.T) {
	ctx := context.Background()

	b, db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed the standard SQLite tuple schema with a single row. The subject is stored
	// discretely across the user_* columns, matching what the dialect projects.
	if _, err := db.ExecContext(ctx, `CREATE TABLE tuple (
		store TEXT NOT NULL,
		object_type TEXT NOT NULL,
		object_id TEXT NOT NULL,
		relation TEXT NOT NULL,
		user_object_type TEXT NOT NULL,
		user_object_id TEXT NOT NULL,
		user_relation TEXT NOT NULL,
		user_type TEXT NOT NULL,
		condition_name TEXT
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tuple
		(store, object_type, object_id, relation, user_object_type, user_object_id, user_relation, user_type, condition_name)
		VALUES ('store1', 'document', 'doc1', 'viewer', 'user', 'alice', '', 'user', NULL)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	a := b.Tuple("a")
	stmt := b.Select(a.SubjectType(), a.SubjectID(), a.SubjectRelation()).
		From(a).
		Where(
			a.Store().Eq(b.Bind("store1")).
				And(a.ObjectType().Eq(b.Bind("document"))).
				And(a.ObjectID().Eq(b.Bind("doc1"))).
				And(a.ObjectRelation().Eq(b.Bind("viewer"))),
		)
	rows, err := b.Build(stmt).Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	t.Cleanup(func() { _ = rows.Close() })

	if !rows.Next() {
		t.Fatalf("expected one row, got none (err: %v)", rows.Err())
	}
	var subjectType, subjectID, subjectRelation string
	if err := rows.Scan(&subjectType, &subjectID, &subjectRelation); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if subjectType != "user" || subjectID != "alice" || subjectRelation != "" {
		t.Errorf("got subject (%q, %q, %q), want (\"user\", \"alice\", \"\")",
			subjectType, subjectID, subjectRelation)
	}
	if rows.Next() {
		t.Errorf("expected exactly one row, got more")
	}
	if err := rows.Err(); err != nil {
		t.Errorf("rows.Err: %v", err)
	}
}
