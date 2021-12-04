package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).e
	db, err := badger.Open(s.initBadgerOptions())
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) initBadgerOptions() badger.Options {
	opts := badger.DefaultOptions
	opts.Dir = s.config.DBPath
	opts.ValueDir = s.config.DBPath
	return opts
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := NewStandAloneStorageReader(s.db)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, data := range batch {
		// cf := data.Cf()
		// switch cf {
		// case engine_util.CfDefault:
		// case engine_util.CfLock:
		// default:
		err := s.writeWithCfDefault(data.Key(), data.Value())
		if err != nil {
			return err
		}
		// }
	}

	return nil
}

func (s *StandAloneStorage) writeWithCfDefault(key []byte, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		txn.Set(key, value)
		return nil
	})
}

type StandAloneStorageReader struct {
	db *badger.DB
}

func NewStandAloneStorageReader(db *badger.DB) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		db: db,
	}
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var value []byte

	err := reader.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		value, err = item.Value()
		return err
	})

	return value, err
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (reader *StandAloneStorageReader) Close() {

}
