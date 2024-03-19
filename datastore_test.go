package dsent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// Use DATASTORE_PROJECT_ID environment variable to set the project ID.
	// And DATASTORE_EMULATOR_HOST to set the emulator host.
	namespace = "Test"
)

// need to implement PropertyLoadSaver
var _ Object = (*exampleObj)(nil)

type exampleObj struct {
	ID            int64 `datastore:"id"`
	Data          int   `datastore:"data,noindex"`
	DelegatedData int   `datastore:"delegated_data,noindex"`
	RealData      int   `datastore:"-"`
	LoadedKey     int64 `datastore:"-"`
}

func (x *exampleObj) LoadKey(k *datastore.Key) error {
	x.LoadedKey = k.ID
	return nil
}

func (x *exampleObj) BuildKey(ns string) (*datastore.Key, error) {
	key := datastore.IDKey("Test", x.ID, nil)
	key.Namespace = ns
	return key, nil
}

// need to implement
func (x *exampleObj) Load(ps []datastore.Property) error {
	// Load I and J as usual.
	if err := datastore.LoadStruct(x, ps); err != nil {
		// ignore ErrFieldMismatch
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			return err
		}
	}

	x.RealData = x.DelegatedData
	return nil
}

func (x *exampleObj) Save() ([]datastore.Property, error) {
	x.DelegatedData = x.RealData
	ps, err := datastore.SaveStruct(x)
	return ps, err
}

type objKeepMissingKey struct {
	ID   int64 `datastore:"id"`
	Data int   `datastore:"data,noindex"`
}

func (x *objKeepMissingKey) BuildKey(ns string) (*datastore.Key, error) {
	return SetNS(datastore.IDKey("Test", x.ID, nil), ns), nil
}

func (x *objKeepMissingKey) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(x, ps)
}

func (x *objKeepMissingKey) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(x)
}

type objDropMissingKey struct {
	ID   int64 `datastore:"id"`
	Data int   `datastore:"data,noindex"`
}

func (x *objDropMissingKey) BuildKey(ns string) (*datastore.Key, error) {
	return SetNS(datastore.IDKey("Test", x.ID, nil), ns), nil
}

// need to implement
func (x *objDropMissingKey) Load(ps []datastore.Property) error {
	// Load I and J as usual.
	if err := datastore.LoadStruct(x, ps); err != nil {
		// ignore ErrFieldMismatch
		if _, ok := err.(*datastore.ErrFieldMismatch); !ok {
			return err
		}
	}
	return nil
}

func (x *objDropMissingKey) Save() ([]datastore.Property, error) {
	ps, err := datastore.SaveStruct(x)
	return ps, err
}

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including assertion methods.
type DSEntTestSuite struct {
	suite.Suite
	*DSEnt[*exampleObj]

	dropMissing *DSEnt[*objDropMissingKey]
	keepMissing *DSEnt[*objKeepMissingKey]

	ctx    context.Context
	cancel func()
}

func (suite *DSEntTestSuite) SetupSuite() {
	projectId := os.Getenv("DATASTORE_PROJECT_ID")
	emulatorHost := os.Getenv("DATASTORE_EMULATOR_HOST")
	if projectId == "" {
		suite.T().Skipf("DATASTORE_PROJECT_ID is not set, skipping test")
	} else if emulatorHost == "" {
		suite.T().Skipf("DATASTORE_EMULATOR_HOST is not set, skipping test")
	}
	suite.T().Logf("Setup Datastore Database in Project(%s) with Namespace(%s)\n", projectId, namespace)
	ctx := context.Background()

	client, err := datastore.NewClient(ctx, "")
	suite.Require().NoError(err)

	entity := NewDSEnt[*exampleObj](client, namespace, "Test")
	suite.DSEnt = entity
	suite.dropMissing = NewDSEnt[*objDropMissingKey](client, namespace, "Test")
	suite.keepMissing = NewDSEnt[*objKeepMissingKey](client, namespace, "Test")

	_, err = suite.purge(ctx)
	suite.Require().NoError(err)
}

func (suite *DSEntTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithTimeout(context.Background(), time.Second*10)
}

func (suite *DSEntTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *DSEntTestSuite) Test01Create() {
	objs := []*exampleObj{}
	for i := 1; i <= 10; i++ {
		obj := &exampleObj{
			ID:       int64(i),
			Data:     i,
			RealData: i,
		}
		objs = append(objs, obj)
	}
	keys, objs, err := suite.BatchCreate(suite.ctx, objs)
	suite.Require().NoError(err)
	for i, k := range keys {
		suite.Require().NotNil(k)
		suite.Require().NotNil(objs[i])
		suite.Require().Equal(objs[i].ID, k.ID)
		suite.Require().Equal(objs[i].Data, i+1)
		suite.Require().Equal(objs[i].RealData, i+1)
		suite.Assert().Equal(objs[i].LoadedKey, k.ID)
	}

	ok, err := suite.Exists(suite.ctx, objs[0])
	fmt.Println(ok, err)
	suite.Require().NoError(err)
	suite.Require().True(ok)

	ok, err = suite.Exists(suite.ctx, &exampleObj{ID: 11})
	fmt.Println(ok, err)
	suite.Require().NoError(err)
	suite.Require().False(ok)
}

func (suite *DSEntTestSuite) Test02CreateFail() {
	_, _, err := suite.Create(suite.ctx, &exampleObj{ID: 1})
	suite.Require().Error(err)
	suite.Equal(status.Code(err), codes.AlreadyExists)
}

func (suite *DSEntTestSuite) Test03Put() {
	key, obj, err := suite.Put(suite.ctx, &exampleObj{ID: 1, Data: 1, RealData: 1})
	suite.Require().NoError(err)
	suite.Require().NotNil(key)
	suite.Require().NotNil(obj)
	suite.Assert().Equal(obj.ID, key.ID)
	suite.Assert().Equal(obj.Data, 1)
	suite.Assert().Equal(obj.DelegatedData, 1)
	suite.Assert().Equal(key.ID, int64(1))
	suite.Assert().Equal(obj.LoadedKey, key.ID)
}

func (suite *DSEntTestSuite) Test04Get() {
	objs := []*exampleObj{}
	for i := 1; i <= 10; i++ {
		obj := &exampleObj{
			ID: int64(i),
		}
		objs = append(objs, obj)
	}

	objs, err := suite.BatchGet(suite.ctx, objs)
	suite.Require().NoError(err)
	for i, obj := range objs {
		suite.Require().NotNil(obj)
		suite.Assert().Equal(obj.ID, int64(i+1))
		suite.Assert().Equal(obj.Data, i+1)
		suite.Assert().Equal(obj.RealData, i+1)
	}
}

func (suite *DSEntTestSuite) Test05UpdateCreate() {
	_, err := suite.Update(suite.ctx, &exampleObj{ID: 1},
		func(eo *exampleObj) (*exampleObj, error) {
			eo.Data = 2
			return eo, nil
		},
		nil,
	)
	suite.Require().NoError(err)

	_, err = suite.Update(suite.ctx, &exampleObj{ID: 11},
		func(eo *exampleObj) (*exampleObj, error) {
			return eo, nil
		},
		nil,
	)
	suite.Require().Error(err)
	suite.Require().ErrorIs(err, datastore.ErrNoSuchEntity)

	customError := errors.New("custom error")
	_, err = suite.Update(suite.ctx, &exampleObj{ID: 11},
		func(eo *exampleObj) (*exampleObj, error) {
			return eo, nil
		},
		func(eo *exampleObj) (*exampleObj, error) {
			return nil, customError
		},
	)
	suite.Require().Error(err)
	suite.Require().ErrorIs(err, customError)

	updated, err := suite.Update(suite.ctx, &exampleObj{ID: 11},
		func(eo *exampleObj) (*exampleObj, error) {
			eo.Data = 11
			return eo, nil
		},
		func(eo *exampleObj) (*exampleObj, error) {
			return &exampleObj{ID: eo.ID}, nil
		},
	)

	suite.Require().NoError(err)
	suite.Require().NotNil(updated)
	suite.Assert().Equal(updated.ID, int64(11))
	suite.Assert().Equal(updated.Data, 11)

	updated, err = suite.Update(suite.ctx, &exampleObj{ID: 1},
		func(eo *exampleObj) (*exampleObj, error) {
			eo.Data = 1
			return eo, nil
		},
		nil,
	)
	suite.Require().NoError(err)
	suite.Require().NotNil(updated)
	suite.Assert().Equal(updated.ID, int64(1))
	suite.Assert().Equal(updated.Data, 1)
}

func (suite *DSEntTestSuite) Test06Delete() {
	err := suite.Delete(suite.ctx, &exampleObj{ID: 11})
	suite.Require().NoError(err)

	err = suite.Delete(suite.ctx, &exampleObj{ID: 11})
	suite.Require().NoError(err)

	_, err = suite.Get(suite.ctx, &exampleObj{ID: 11})
	suite.Require().Error(err)
	suite.Require().ErrorIs(err, datastore.ErrNoSuchEntity)
}

func (suite *DSEntTestSuite) Test07UpdateTx() {
	// tx1
	objs := []*exampleObj{}
	for i := 1; i <= 10; i++ {
		obj := &exampleObj{
			ID: int64(i),
		}
		objs = append(objs, obj)
	}

	_, err := suite.RunInTransaction(suite.ctx, func(tx *datastore.Transaction) error {
		for _, obj := range objs {
			if _, err := suite.UpdateTx(tx, &exampleObj{ID: obj.ID},
				func(eo *exampleObj) (*exampleObj, error) {
					eo.Data += 1
					return eo, nil
				},
				nil,
			); err != nil {
				return err
			}
			// slow it down
			time.Sleep(time.Millisecond * 100)
		}
		time.Sleep(time.Second)
		return nil
	})
	suite.NoError(err)
}

func (suite *DSEntTestSuite) Test97UpdateConflict() {
	suite.BatchCreate(suite.ctx, []*exampleObj{
		{ID: 1, Data: 1},
	})

	// tx1
	var errA error
	go func() {
		_, errA = suite.RunInTransaction(suite.ctx, func(tx *datastore.Transaction) error {
			if _, err := suite.UpdateTx(tx, &exampleObj{ID: 1},
				func(eo *exampleObj) (*exampleObj, error) {
					eo.Data += 1
					time.Sleep(10 * time.Second)
					return eo, nil
				},
				nil,
			); err != nil {
				return err
			}
			return nil
		})
	}()
	time.Sleep(time.Second)
	// tx2
	var errB error
	go func() {
		_, errB = suite.RunInTransaction(suite.ctx, func(tx *datastore.Transaction) error {
			if _, err := suite.UpdateTx(tx, &exampleObj{ID: 1},
				func(eo *exampleObj) (*exampleObj, error) {
					eo.Data += 1
					time.Sleep(10 * time.Second)
					return eo, nil
				},
				nil,
			); err != nil {
				return err
			}
			return nil
		})
	}()

	suite.NoError(errA)
	suite.NoError(errB)
}

func (suite *DSEntTestSuite) Test98ShouldMismatch() {
	obj, err := suite.keepMissing.Get(suite.ctx, &objKeepMissingKey{ID: 1})
	suite.Require().Error(err)
	mismatch := &datastore.ErrFieldMismatch{}
	suite.Require().ErrorAs(err, &mismatch)
	suite.Equal(obj.ID, int64(1))
	suite.Equal(2, obj.Data)
}

func (suite *DSEntTestSuite) Test99DropMismatch() {
	obj, err := suite.dropMissing.Get(suite.ctx, &objDropMissingKey{ID: 1})
	suite.Require().NoError(err)
	suite.Equal(obj.ID, int64(1))
	suite.Equal(2, obj.Data)
}

func (suite *DSEntTestSuite) purge(ctx context.Context) (int, error) {
	sum := 0
	q := datastore.NewQuery("").Namespace(namespace).KeysOnly().Limit(500)

	for i := 0; ; i++ {
		keys, err := suite.DSEnt.GetAll(ctx, q, nil)
		if err != nil {
			return sum, fmt.Errorf("failed to get keys: %w", err)
		}
		deleteKeys := make([]*datastore.Key, 0, len(keys))
		for _, k := range keys {
			if !strings.HasPrefix(k.Kind, "__") {
				deleteKeys = append(deleteKeys, k)
			}
		}
		if len(deleteKeys) == 0 {
			break
		}
		sum += len(deleteKeys)
		if err := suite.DSEnt.DeleteMulti(ctx, deleteKeys); err != nil {
			return sum, fmt.Errorf("failed to delete keys: %w", err)
		}
	}
	return sum, nil
}

func (suite *DSEntTestSuite) TearDownSuite() {
	suite.T().Logf("TearDown Datastore Database with Namespace(%s)\n", namespace)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	sum, err := suite.purge(ctx)
	suite.Require().NoError(err)

	suite.Close()

	suite.T().Logf("TearDown completed: %d keys deleted in total\n", sum)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestDSEnt(t *testing.T) {
	suite.Run(t, new(DSEntTestSuite))
}
