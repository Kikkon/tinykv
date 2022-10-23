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
	badgerDB *badger.DB
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil {
		return []byte(nil), nil
	}
	return v, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	option := badger.DefaultOptions
	option.Dir = "/tmp/badger"
	option.ValueDir = "/tmp/badger"
	badgerDB, e := badger.Open(option)
	if e != nil {
		panic(e)
	}
	return &StandAloneStorage{
		badgerDB: badgerDB,
	}
}

func (s *StandAloneStorage) Start() error {

	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.badgerDB.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.badgerDB.NewTransaction(true)
	return &StandAloneStorageReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.badgerDB.NewTransaction(true)

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.badgerDB, m.Cf(), m.Key(), m.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.badgerDB, m.Cf(), m.Key())
			if err != nil {
				return err
			}
		}
	}
	err := txn.Commit()
	if err != nil {
		return err
	}
	return nil
}
