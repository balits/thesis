package durable

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/balits/kave/internal/storage"
	"github.com/balits/kave/internal/storage/bytestore"
	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
)

const dbFileName = "bolt.db"

type boltStore struct {
	db   *bolt.DB
	file string
	sz   atomic.Int64

	// mapping from string buckets to byte buckets
	// very low overhead since there isnt a bunch of buckes anyways
	// but this way dont gotta cast (allocate) on every operation
	bucketMap map[storage.Bucket][]byte
}

func NewStore(opts storage.Options) (bytestore.ByteStore, error) {
	if opts.Kind != storage.StorageKindBoltdb {
		return nil, fmt.Errorf("failed to create boltdb store: option 'Kind' was not StorageKindBoltdb")
	}

	dbpath := filepath.Join(opts.Dir, dbFileName)
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
		NoSync:  true,
	})
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	bucketMap := make(map[storage.Bucket][]byte, len(opts.InitialBuckets))
	for _, bucket := range opts.InitialBuckets {
		_, err := tx.CreateBucket([]byte(bucket))
		if err != nil && !errors.Is(err, bolterrors.ErrBucketExists) {
			return nil, fmt.Errorf("failed to create boltdb store: %v", err)
		}
		bucketMap[bucket] = []byte(bucket)
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &boltStore{
		db:        db,
		file:      dbpath,
		bucketMap: bucketMap,
	}, nil
}

func (s *boltStore) Close() error {
	return s.db.Close()
}

func (s *boltStore) Get(bucket storage.Bucket, key []byte) (value []byte, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}

		return
	}

	return
}

func (s *boltStore) Put(bucket storage.Bucket, key, value []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, storage.ErrEmptyKey
	}

	var oldValue []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		oldValue = copyBytes(bucket.Get(key))

		return bucket.Put(key, value)
	})

	// wrap any non bucket errors into ErrInternalStorageError,
	// so that the caller can decide if it wants to retry or not based on the error type
	if err != nil && err != storage.ErrBucketNotFound {
		return oldValue, fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
	} else if err != nil {
		return oldValue, err
	}
	return oldValue, nil
}

func (s *boltStore) Delete(bucket storage.Bucket, key []byte) (value []byte, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		value = copyBytes(bucket.Get(key))
		return bucket.Delete(key)
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}

		return
	}

	if value == nil {
		return
	}

	return
}

func (s *boltStore) PrefixScan(bucket storage.Bucket, prefix []byte, f func(key, value []byte) bool) error {
	g := func(key, value []byte) bool {
		if !bytes.HasPrefix(key, prefix) {
			return false
		}

		if !f(key, value) {
			return false
		}
		return true
	}

	err := s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if !g(copyBytes(k), copyBytes(v)) {
				continue
			}
		}

		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}
		return err
	}

	return nil
}

func (s *boltStore) Scan(bucket storage.Bucket, f func(key, value []byte) bool) error {
	err := s.db.View(func(tx *bolt.Tx) error {
		bucketBytes, ok := s.bucketMap[bucket]
		if !ok {
			return storage.ErrBucketNotFound
		}
		bucket := tx.Bucket(bucketBytes)
		if bucket == nil {
			return storage.ErrBucketNotFound
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if !f(copyBytes(k), copyBytes(v)) {
				break
			}
		}

		return nil
	})

	if err != nil {
		if err != storage.ErrBucketNotFound {
			err = fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
		}
		return err
	}

	return nil
}

func (s *boltStore) NewBatch() (bytestore.Batch, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", storage.ErrInternalStorageError, err)
	}
	return newBatch(tx, s.bucketMap, &s.sz), nil
}

// we dont really know how many bytes were written, and it doesnt matter for us, so we just return 0
func (s *boltStore) WriteTo(w io.Writer) (int64, error) {
	return 0, encode(w, s)
}

// we dont really know how many bytes were written, and it doesnt matter for us, so we just return 0
func (s *boltStore) ReadFrom(r io.Reader) (int64, error) {
	db, err := decode(r, s.file)
	if err != nil {
		return 0, fmt.Errorf("failed to decode reader: %v", err)
	}

	s.db = db
	return 0, nil
}

func (s *boltStore) SizeBytes() int64 {
	return s.sz.Load()
}

func (s *boltStore) Defragment() error {
	newpath := s.file + ".defrag"

	f, err := os.Create(newpath)
	if err != nil {
		return fmt.Errorf("create failed: %w", err)
	}

	err = s.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(f)
		return err
	})
	if syncErr := f.Sync(); syncErr != nil && err == nil {
		err = syncErr
	}
	_ = f.Close()

	if err != nil {
		_ = os.Remove(newpath)
		return fmt.Errorf("writeTo/fsync failed: %w", err)
	}

	if err := s.db.Close(); err != nil {
		_ = os.Remove(newpath)
		return fmt.Errorf("close failed: %w", err)
	}

	if err := os.Rename(newpath, s.file); err != nil {
		return fmt.Errorf("rename failed: %w", err)
	}

	db, err := bolt.Open(s.file, 0600, nil)
	if err != nil {
		return fmt.Errorf("reopen failed: %w", err)
	}

	s.db = db
	return nil
}

func (s *boltStore) Ping() error {
	// we need to see if the db is redy to respond to requests, which is especially important for the first few moments after startup, when the db file might exist but not be ready yet
	return s.db.View(func(tx *bolt.Tx) error {
		return nil
	})
}

func copyBytes(in []byte) (out []byte) {
	if in == nil {
		return nil
	}
	return append(out, in...)
}
