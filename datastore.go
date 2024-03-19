// Package dsent provides a wrapper around the Google Cloud Datastore client
// to simplify common operations for working with entities in Datastore.
package dsent

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
)

// ErrKeyChanged is returned when the key of an entity has changed during a transaction.
var ErrKeyChanged = errors.New("key changed during transaction")

// ErrNotFound is returned when an entity is not found in Datastore.
var ErrNotFound = datastore.ErrNoSuchEntity

// ErrConcurrentTransaction is returned when a concurrent transaction is detected.
var ErrConcurrentTransaction = datastore.ErrConcurrentTransaction

// ErrUpdateAbort is returned when an update operation is aborted.
var ErrUpdateAbort = errors.New("update aborted")

// Object is an interface that represents an entity in Datastore.
type Object interface {
	datastore.PropertyLoadSaver
	BuildKey(ns string) (*datastore.Key, error)
}

// DSEnt is a generic Datastore entity wrapper that provides methods for
// performing common operations on entities.
type DSEnt[T Object] struct {
	*datastore.Client
	namespace string
	kind      string
}

// NewDSEnt creates a new instance of DSEnt with the given Datastore client and namespace.
func NewDSEnt[T Object](client *datastore.Client, ns string, kind string) *DSEnt[T] {
	return &DSEnt[T]{
		Client:    client,
		namespace: ns,
		kind:      kind,
	}
}

// SetNS is a helper sets the namespace of a Datastore key and its parent keys.
func SetNS(key *datastore.Key, ns string) *datastore.Key {
	key.Namespace = ns
	next := key.Parent
	for next != nil {
		next.Namespace = ns
		next = next.Parent
	}
	return key
}

// cmpKey compares two Datastore keys and returns true if they are equal.
func (db *DSEnt[T]) cmpKey(a, b *datastore.Key) bool {
	left, right := a, b
	for {
		if left.Namespace != right.Namespace {
			return false
		} else if left.Kind != right.Kind {
			return false
		} else if left.ID != right.ID {
			return false
		} else if left.Name != right.Name {
			return false
		}
		left, right = left.Parent, right.Parent
		if left == nil {
			return right == nil
		} else if right == nil {
			return false
		}
	}
}

// buildKeys builds Datastore keys for a slice of objects.
func (db *DSEnt[T]) buildKeys(objs []T) ([]*datastore.Key, error) {
	keys := make([]*datastore.Key, len(objs))
	for i, obj := range objs {
		key, err := obj.BuildKey(db.namespace)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}
	return keys, nil
}

// ResolveKey resolves the key of an object by calling its LoadKey method.
func (db *DSEnt[T]) ResolveKey(key *datastore.Key, obj T) error {
	inter := interface{}(obj)
	if e, ok := inter.(datastore.KeyLoader); ok {
		if err := e.LoadKey(key); err != nil {
			return err
		}
	}
	return nil
}

// Create creates a new entity in Datastore.
func (db *DSEnt[T]) Create(ctx context.Context, obj T) (*datastore.Key, T, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return nil, obj, err
	}
	mut := datastore.NewInsert(key, obj)
	keys, err := db.Client.Mutate(ctx, mut)
	if err != nil {
		return nil, obj, err
	}
	key = keys[0]
	return key, obj, db.ResolveKey(key, obj)
}

// BatchCreate creates multiple entities in Datastore within a transaction.
func (db *DSEnt[T]) BatchCreate(ctx context.Context, objs []T) ([]*datastore.Key, []T, error) {
	var pks []*datastore.PendingKey
	var err error
	cmt, err := db.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		pks, objs, err = db.BatchCreateTx(tx, objs)
		return err
	})

	if err != nil {
		return nil, objs, err
	}
	keys := make([]*datastore.Key, len(pks))
	for i, pk := range pks {
		keys[i] = cmt.Key(pk)
	}
	var loadKeyErr error
	for i, key := range keys {
		if err := db.ResolveKey(key, objs[i]); err != nil {
			loadKeyErr = err
		}
	}
	return keys, objs, loadKeyErr
}

// CreateTx creates a new entity in Datastore within a transaction.
func (db *DSEnt[T]) CreateTx(tx *datastore.Transaction, obj T) (*datastore.PendingKey, T, error) {
	pks, objs, err := db.BatchCreateTx(tx, []T{obj})
	if err != nil {
		return nil, obj, err
	}
	return pks[0], objs[0], nil
}

// BatchCreateTx creates multiple entities in Datastore within a transaction.
func (db *DSEnt[T]) BatchCreateTx(tx *datastore.Transaction, objs []T) ([]*datastore.PendingKey, []T, error) {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return nil, objs, err
	}
	muts := make([]*datastore.Mutation, len(objs))
	for i, obj := range objs {
		muts[i] = datastore.NewInsert(keys[i], obj)
	}
	pks, err := tx.Mutate(muts...)
	if err != nil {
		return nil, objs, err
	}
	return pks, objs, nil
}

// Exists checks if an entity exists in Datastore.
func (db *DSEnt[T]) Exists(ctx context.Context, obj T) (bool, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return false, err
	}
	q := datastore.NewQuery(key.Kind).Namespace(db.namespace).FilterField("__key__", "=", key).KeysOnly().Limit(1)
	keys, err := db.Client.GetAll(ctx, q, nil)
	if err != nil {
		return false, err
	}
	return len(keys) > 0, nil
}

// ExistsTx checks if an entity exists in Datastore within a transaction.
func (db *DSEnt[T]) ExistsTx(ctx context.Context, tx *datastore.Transaction, obj T) (bool, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return false, err
	}
	q := datastore.NewQuery(key.Kind).Namespace(db.namespace).FilterField("__key__", "=", key).KeysOnly().Limit(1).Transaction(tx)
	keys, err := db.Client.GetAll(ctx, q, nil)
	if err != nil {
		return false, err
	}
	return len(keys) > 0, nil
}

// Get retrieves an entity from Datastore and populates the input object with the retrieved data.
func (db *DSEnt[T]) Get(ctx context.Context, obj T) (T, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			err = merr[0]
		}
		return obj, err
	}
	err = db.Client.Get(ctx, key, obj)
	return obj, err
}

// GetTx retrieves an entity from Datastore within a transaction and populates the input object with the retrieved data.
func (db *DSEnt[T]) GetTx(tx *datastore.Transaction, obj T) (T, error) {
	objs, err := db.BatchGetTx(tx, []T{obj})
	if err != nil {
		if merr, ok := err.(datastore.MultiError); ok {
			err = merr[0]
		}
		return obj, err
	}
	return objs[0], nil
}

// BatchGet retrieves multiple entities from Datastore.
func (db *DSEnt[T]) BatchGet(ctx context.Context, objs []T) ([]T, error) {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return objs, err
	}
	if err := db.Client.GetMulti(ctx, keys, objs); err != nil {
		return objs, err
	}
	return objs, nil
}

// BatchGetTx retrieves multiple entities from Datastore within a transaction.
func (db *DSEnt[T]) BatchGetTx(tx *datastore.Transaction, objs []T) ([]T, error) {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return objs, err
	}
	if err := tx.GetMulti(keys, objs); err != nil {
		return objs, err
	}
	return objs, nil
}

// Put saves an entity to Datastore.
func (db *DSEnt[T]) Put(ctx context.Context, obj T) (*datastore.Key, T, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return nil, obj, err
	}
	key, err = db.Client.Put(ctx, key, obj)
	if err != nil {
		return nil, obj, err
	}
	return key, obj, db.ResolveKey(key, obj)
}

// BatchPut saves multiple entities to Datastore within a transaction.
func (db *DSEnt[T]) BatchPut(ctx context.Context, objs []T) ([]*datastore.Key, []T, error) {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return nil, objs, err
	}
	var pks []*datastore.PendingKey
	cmt, err := db.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		pks, objs, err = db.BatchPutTx(tx, objs)
		return err
	})
	if err != nil {
		return nil, objs, err
	}

	for i, pk := range pks {
		keys[i] = cmt.Key(pk)
	}
	var loadKeyErr error
	for i, key := range keys {
		if err := db.ResolveKey(key, objs[i]); err != nil {
			loadKeyErr = err
		}
	}
	return keys, objs, loadKeyErr
}

// PutTx saves a single entity to Datastore within a transaction.
func (db *DSEnt[T]) PutTx(tx *datastore.Transaction, obj T) (*datastore.PendingKey, T, error) {
	pks, objs, err := db.BatchPutTx(tx, []T{obj})
	if err != nil {
		return nil, obj, err
	}
	return pks[0], objs[0], nil
}

// BatchPutTx saves multiple entities to Datastore within a transaction.
func (db *DSEnt[T]) BatchPutTx(tx *datastore.Transaction, objs []T) ([]*datastore.PendingKey, []T, error) {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return nil, objs, err
	}
	muts := make([]*datastore.Mutation, len(objs))
	for i, obj := range objs {
		muts[i] = datastore.NewUpsert(keys[i], obj)
	}
	pks, err := tx.Mutate(muts...)
	if err != nil {
		return nil, objs, err
	}
	return pks, objs, nil
}

// Update updates an entity in Datastore within a transaction.
func (db *DSEnt[T]) Update(
	ctx context.Context, obj T,
	updateFunc func(T) (T, error),
	createFunc func(T) (T, error),
) (T, error) {
	var err error
	_, err = db.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		obj, err = db.UpdateTx(tx, obj, updateFunc, createFunc)
		return err
	})
	if err != nil {
		return obj, err
	}
	return obj, nil
}

// UpdateTx updates an entity in Datastore within a transaction.
func (db *DSEnt[T]) UpdateTx(
	tx *datastore.Transaction, obj T,
	updateFunc func(T) (T, error),
	createFunc func(T) (T, error),
) (T, error) {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return obj, err
	}
	created := false
	if err := tx.Get(key, obj); err == datastore.ErrNoSuchEntity {
		if createFunc == nil {
			return obj, err
		}
		obj, err = createFunc(obj)
		if err != nil {
			return obj, err
		}
		if newKey, err := obj.BuildKey(db.namespace); err == nil {
			if !db.cmpKey(key, newKey) {
				return obj, ErrKeyChanged
			}
		} else {
			return obj, err
		}
		created = true
	} else if err != nil {
		return obj, err
	}

	if obj, err = updateFunc(obj); errors.Is(err, ErrUpdateAbort) {
		return obj, nil
	} else if err != nil {
		return obj, err
	}
	if newKey, err := obj.BuildKey(db.namespace); err == nil {
		if !db.cmpKey(key, newKey) {
			return obj, ErrKeyChanged
		}
	} else {
		return obj, err
	}

	var mut *datastore.Mutation
	if created {
		mut = datastore.NewInsert(key, obj)
	} else {
		mut = datastore.NewUpdate(key, obj)
	}
	if _, err := tx.Mutate(mut); err != nil {
		return obj, err
	}
	return obj, nil
}

// Delete deletes an entity from Datastore.
func (db *DSEnt[T]) Delete(ctx context.Context, obj T) error {
	key, err := obj.BuildKey(db.namespace)
	if err != nil {
		return err
	}
	return db.Client.Delete(ctx, key)
}

// DeleteTx deletes an entity from Datastore within a transaction.
func (db *DSEnt[T]) DeleteTx(tx *datastore.Transaction, obj T) error {
	return db.BatchDeleteTx(tx, []T{obj})
}

// BatchDelete is transactional batch delete.
func (db *DSEnt[T]) BatchDelete(ctx context.Context, objs []T) error {
	if _, err := db.Client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		return db.BatchDeleteTx(tx, objs)
	}); err != nil {
		return err
	}
	return nil
}

// BatchDeleteTx is used to delete multiple entities in a transaction.
func (db *DSEnt[T]) BatchDeleteTx(tx *datastore.Transaction, objs []T) error {
	keys, err := db.buildKeys(objs)
	if err != nil {
		return err
	}
	return tx.DeleteMulti(keys)
}

func (db *DSEnt[T]) Close() {
	db.Client.Close()
}

func (db *DSEnt[T]) Namespace() string {
	return db.namespace
}

func (db *DSEnt[T]) NewQuery() *datastore.Query {
	return datastore.NewQuery(db.kind).Namespace(db.namespace)
}
