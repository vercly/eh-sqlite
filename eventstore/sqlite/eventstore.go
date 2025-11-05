package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vercly/eh-sqlite/context/sqlite"

	json "github.com/json-iterator/go"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/uuid"
)

// EventStore is an eventhorizon.EventStore for SQLite.
type EventStore struct {
	db                    *sql.DB
	eventsTable           string
	streamsTable          string
	snapshotsTable        string
	eventHandlerAfterSave eh.EventHandler
	eventHandlerInTX      eh.EventHandler

	// Prepared statements
	stmtInsertEvent     *sql.Stmt
	stmtSelectEvents    *sql.Stmt
	stmtSelectStream    *sql.Stmt
	stmtInsertStream    *sql.Stmt
	stmtUpdateStream    *sql.Stmt
	stmtUpdateAllStream *sql.Stmt
	stmtInsertSnapshot  *sql.Stmt
	stmtSelectSnapshot  *sql.Stmt
}

// NewEventStore creates a new EventStore.
func NewEventStore(db *sql.DB, options ...Option) (*EventStore, error) {
	s := &EventStore{
		db:             db,
		eventsTable:    "events",
		streamsTable:   "streams",
		snapshotsTable: "snapshots",
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	// Create events table.
	if _, err := s.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
			position INTEGER PRIMARY KEY AUTOINCREMENT,
			event_type TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			aggregate_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			data TEXT NOT NULL,
			metadata TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_%[1]s_aggregate ON %[1]s (aggregate_id, version);
	`, s.eventsTable)); err != nil {
		return nil, fmt.Errorf("could not create events table: %w", err)
	}

	// Create streams table.
	if _, err := s.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			aggregate_id TEXT PRIMARY KEY,
			position INTEGER NOT NULL,
			aggregate_type TEXT NOT NULL,
			version INTEGER NOT NULL,
			updated_at DATETIME NOT NULL
		)
	`, s.streamsTable)); err != nil {
		return nil, fmt.Errorf("could not create streams table: %w", err)
	}

	// Create snapshots table.
	if _, err := s.db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			aggregate_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			data TEXT NOT NULL,
			timestamp DATETIME NOT NULL,
			aggregate_type TEXT NOT NULL,
			PRIMARY KEY (aggregate_id, version)
		)
	`, s.snapshotsTable)); err != nil {
		return nil, fmt.Errorf("could not create snapshots table: %w", err)
	}

	// Create the $all stream if it doesn't exist.
	if _, err := s.db.Exec(fmt.Sprintf(`INSERT OR IGNORE INTO %s (aggregate_id, position, aggregate_type, version, updated_at) VALUES (?, ?, ?, ?, ?)`, s.streamsTable),
		"$all", 0, "", 0, time.Now()); err != nil {
		return nil, fmt.Errorf("could not create $all stream: %w", err)
	}

	// Prepare statements
	var err error
	if s.stmtInsertEvent, err = s.db.Prepare(fmt.Sprintf(`INSERT INTO %s (position, event_type, timestamp, aggregate_type, aggregate_id, version, data, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, s.eventsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare insert event statement: %w", err)
	}
	if s.stmtSelectEvents, err = s.db.Prepare(fmt.Sprintf(`SELECT event_type, data, timestamp, aggregate_type, aggregate_id, version, metadata FROM %s WHERE aggregate_id = ? AND version >= ? ORDER BY version ASC`, s.eventsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare select events statement: %w", err)
	}
	if s.stmtSelectStream, err = s.db.Prepare(fmt.Sprintf(`SELECT position FROM %s WHERE aggregate_id = ?`, s.streamsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare select stream statement: %w", err)
	}
	if s.stmtInsertStream, err = s.db.Prepare(fmt.Sprintf(`INSERT INTO %s (aggregate_id, position, aggregate_type, version, updated_at) VALUES (?, ?, ?, ?, ?)`, s.streamsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare insert stream statement: %w", err)
	}
	if s.stmtUpdateStream, err = s.db.Prepare(fmt.Sprintf(`UPDATE %s SET position = ?, version = version + ?, updated_at = ? WHERE aggregate_id = ? AND version = ?`, s.streamsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare update stream statement: %w", err)
	}
	if s.stmtUpdateAllStream, err = s.db.Prepare(fmt.Sprintf(`UPDATE %s SET position = ? WHERE aggregate_id = ?`, s.streamsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare update all stream statement: %w", err)
	}
	if s.stmtInsertSnapshot, err = s.db.Prepare(fmt.Sprintf(`INSERT INTO %s (aggregate_id, version, data, timestamp, aggregate_type) VALUES (?, ?, ?, ?, ?)`, s.snapshotsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare insert snapshot statement: %w", err)
	}
	if s.stmtSelectSnapshot, err = s.db.Prepare(fmt.Sprintf(`SELECT data, version, timestamp, aggregate_type FROM %s WHERE aggregate_id = ? ORDER BY version DESC LIMIT 1`, s.snapshotsTable)); err != nil {
		return nil, fmt.Errorf("could not prepare select snapshot statement: %w", err)
	}

	return s, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithEventHandler adds an event handler that will be called after saving events.
func WithEventHandler(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}
		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}
		s.eventHandlerAfterSave = h
		return nil
	}
}

// WithEventHandlerInTX adds an event handler that will be called during saving of events.
func WithEventHandlerInTX(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		if s.eventHandlerAfterSave != nil {
			return fmt.Errorf("another event handler is already set")
		}
		if s.eventHandlerInTX != nil {
			return fmt.Errorf("another TX event handler is already set")
		}
		s.eventHandlerInTX = h
		return nil
	}
}

// WithTableNames uses different table names for the events, streams and snapshots.
// The table names are not sanitized and could be vulnerable to SQL injection.
func WithTableNames(events, streams, snapshots string) Option {
	return func(s *EventStore) error {
		s.eventsTable = events
		s.streamsTable = streams
		s.snapshotsTable = snapshots
		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	id := events[0].AggregateID()
	at := events[0].AggregateType()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not begin transaction: %w", err),
			Op:  eh.EventStoreOpSave,
		}
	}
	defer tx.Rollback() // Rollback on any error.

	// Fetch and increment global version in the all-stream.
	row := tx.Stmt(s.stmtSelectStream).QueryRow("$all")
	var allStreamPosition int
	if err := row.Scan(&allStreamPosition); err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not get global position: %w", err),
			Op:  eh.EventStoreOpSave,
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]*evt, len(events))
	for i, event := range events {
		if event.AggregateID() != id {
			return &eh.EventStoreError{Err: eh.ErrMismatchedEventAggregateIDs}
		}
		if event.AggregateType() != at {
			return &eh.EventStoreError{Err: eh.ErrMismatchedEventAggregateTypes}
		}
		if event.Version() != originalVersion+i+1 {
			return &eh.EventStoreError{Err: eh.ErrIncorrectEventVersion}
		}

		e, err := newEvt(ctx, event)
		if err != nil {
			return &eh.EventStoreError{
				Err: fmt.Errorf("could not create event record: %w", err),
				Op:  eh.EventStoreOpSave,
			}
		}
		e.Position = allStreamPosition + i + 1
		dbEvents[i] = e
	}

	// Insert events.
	stmt := tx.Stmt(s.stmtInsertEvent)
	var lastPosition int
	for _, e := range dbEvents {
		lastPosition = e.Position
		_, err := stmt.Exec(e.Position, e.EventType, e.Timestamp, e.AggregateType, e.AggregateID, e.Version, e.RawData, e.Metadata)
		if err != nil {
			return &eh.EventStoreError{
				Err: fmt.Errorf("could not insert event: %w", err),
				Op:  eh.EventStoreOpSave,
			}
		}
	}

	// Update the $all stream.
	if _, err := tx.Stmt(s.stmtUpdateAllStream).Exec(lastPosition, "$all"); err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not update global position: %w", err),
			Op:  eh.EventStoreOpSave,
		}
	}

	// Update the aggregate stream.
	if originalVersion == 0 {
		if _, err := tx.Stmt(s.stmtInsertStream).Exec(id, lastPosition, at, len(dbEvents), time.Now()); err != nil {
			return &eh.EventStoreError{
				Err: fmt.Errorf("could not insert stream: %w", err),
				Op:  eh.EventStoreOpSave,
			}
		}
	} else {
		res, err := tx.Stmt(s.stmtUpdateStream).Exec(lastPosition, len(dbEvents), time.Now(), id, originalVersion)
		if err != nil {
			return &eh.EventStoreError{
				Err: fmt.Errorf("could not update stream: %w", err),
				Op:  eh.EventStoreOpSave,
			}
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return &eh.EventStoreError{
				Err: fmt.Errorf("could not get rows affected: %w", err),
				Op:  eh.EventStoreOpSave,
			}
		}
		if rowsAffected == 0 {
			return &eh.EventStoreError{Err: eh.ErrEventConflictFromOtherSave}
		}
	}

	if s.eventHandlerInTX != nil {
		// propagate tx context
		txCtx := sqlite.NewContextWithTx(ctx, tx)
		for _, e := range events {
			if err := s.eventHandlerInTX.HandleEvent(txCtx, e); err != nil {
				return &eh.EventHandlerError{Err: err, Event: e}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not commit transaction: %w", err),
			Op:  eh.EventStoreOpSave,
		}
	}

	if s.eventHandlerAfterSave != nil {
		for _, e := range events {
			if err := s.eventHandlerAfterSave.HandleEvent(ctx, e); err != nil {
				return &eh.EventHandlerError{Err: err, Event: e}
			}
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	return s.LoadFrom(ctx, id, 1)
}

// LoadFrom loads all events from version for the aggregate id from the store.
func (s *EventStore) LoadFrom(ctx context.Context, id uuid.UUID, version int) ([]eh.Event, error) {
	rows, err := s.stmtSelectEvents.Query(id.String(), version)
	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not query events: %w", err),
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}
	defer rows.Close()

	events := []eh.Event{}
	for rows.Next() {
		var eventType, aggregateType, aggregateID, metadata string
		var data []byte
		var timestamp time.Time
		var v int
		if err := rows.Scan(&eventType, &data, &timestamp, &aggregateType, &aggregateID, &v, &metadata); err != nil {
			return nil, &eh.EventStoreError{
				Err:         fmt.Errorf("could not scan event: %w", err),
				Op:          eh.EventStoreOpLoad,
				AggregateID: id,
			}
		}

		e, err := s.toEvent(eventType, data, timestamp, aggregateType, aggregateID, v, metadata)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	if len(events) == 0 {
		return nil, &eh.EventStoreError{
			Err:         eh.ErrAggregateNotFound,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	return events, nil
}

func (s *EventStore) LoadSnapshot(ctx context.Context, id uuid.UUID) (*eh.Snapshot, error) {
	row := s.stmtSelectSnapshot.QueryRow(id.String())

	var data string
	var version int
	var timestamp time.Time
	var aggregateTypeStr string
	if err := row.Scan(&data, &version, &timestamp, &aggregateTypeStr); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No snapshot found.
		}
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not scan snapshot: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	aggregateType := eh.AggregateType(aggregateTypeStr)
	snapshotData, err := eh.CreateSnapshotData(id, aggregateType)
	if err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not create snapshot data: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	if err := json.NewDecoder(strings.NewReader(data)).Decode(&snapshotData); err != nil {
		return nil, &eh.EventStoreError{
			Err:         fmt.Errorf("could not unmarshal snapshot: %w", err),
			Op:          eh.EventStoreOpLoadSnapshot,
			AggregateID: id,
		}
	}

	snapshot := &eh.Snapshot{
		Version:       version,
		Timestamp:     timestamp,
		AggregateType: aggregateType,
		State:         snapshotData,
	}

	return snapshot, nil
}

func (s *EventStore) SaveSnapshot(ctx context.Context, id uuid.UUID, snapshot eh.Snapshot) error {
	if isSnapshotEmpty(snapshot) {
		return &eh.EventStoreError{
			Err: fmt.Errorf("snapshot is empty"),
			Op:  eh.EventStoreOpSaveSnapshot,
		}
	}

	marshaledData, err := json.Marshal(snapshot.State)
	if err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not marshal snapshot: %w", err),
			Op:  eh.EventStoreOpSaveSnapshot,
		}
	}

	_, err = s.stmtInsertSnapshot.Exec(id.String(), snapshot.Version, string(marshaledData), time.Now(), snapshot.AggregateType)
	if err != nil {
		return &eh.EventStoreError{
			Err: fmt.Errorf("could not insert snapshot: %w", err),
			Op:  eh.EventStoreOpSaveSnapshot,
		}
	}

	return nil
}

// Close implements the Close method of the eventhorizon.EventStore interface.
func (s *EventStore) Close() error {
	if s.stmtInsertEvent != nil {
		s.stmtInsertEvent.Close()
	}
	if s.stmtSelectEvents != nil {
		s.stmtSelectEvents.Close()
	}
	if s.stmtSelectStream != nil {
		s.stmtSelectStream.Close()
	}
	if s.stmtInsertStream != nil {
		s.stmtInsertStream.Close()
	}
	if s.stmtUpdateStream != nil {
		s.stmtUpdateStream.Close()
	}
	if s.stmtUpdateAllStream != nil {
		s.stmtUpdateAllStream.Close()
	}
	if s.stmtInsertSnapshot != nil {
		s.stmtInsertSnapshot.Close()
	}
	if s.stmtSelectSnapshot != nil {
		s.stmtSelectSnapshot.Close()
	}
	return s.db.Close()
}

func isSnapshotEmpty(s eh.Snapshot) bool {
	return s.AggregateType == "" || s.State == nil || s.Timestamp.IsZero()
}

// evt is the internal event record for the SQLite event store.
type evt struct {
	Position      int
	EventType     eh.EventType
	Timestamp     time.Time
	AggregateType eh.AggregateType
	AggregateID   uuid.UUID
	Version       int
	RawData       json.RawMessage
	Metadata      json.RawMessage
}

// newEvt returns a new evt for an event.
func newEvt(ctx context.Context, event eh.Event) (*evt, error) {
	data, err := json.Marshal(event.Data())
	if err != nil {
		return nil, fmt.Errorf("could not marshal event data: %w", err)
	}

	metadata, err := json.Marshal(event.Metadata())
	if err != nil {
		return nil, fmt.Errorf("could not marshal event metadata: %w", err)
	}

	return &evt{
		EventType:     event.EventType(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
		RawData:       data,
		Metadata:      metadata,
	}, nil
}

func (s *EventStore) toEvent(eventType string, data []byte, timestamp time.Time, aggregateType string, aggregateID string, version int, metadata string) (eh.Event, error) {
	eventData, err := eh.CreateEventData(eh.EventType(eventType))
	if err != nil {
		return nil, &eh.EventStoreError{
			Err: fmt.Errorf("could not create event data: %w", err),
			Op:  eh.EventStoreOpLoad,
		}
	}
	if err := json.Unmarshal(data, &eventData); err != nil {
		return nil, &eh.EventStoreError{
			Err: fmt.Errorf("could not unmarshal event data: %w", err),
			Op:  eh.EventStoreOpLoad,
		}
	}

	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(metadata), &meta); err != nil {
		return nil, &eh.EventStoreError{
			Err: fmt.Errorf("could not unmarshal metadata: %w", err),
			Op:  eh.EventStoreOpLoad,
		}
	}

	return eh.NewEvent(
		eh.EventType(eventType),
		eventData,
		timestamp,
		eh.ForAggregate(
			eh.AggregateType(aggregateType),
			uuid.MustParse(aggregateID),
			version,
		),
		eh.WithMetadata(meta),
	), nil
}
