package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	resp.Value = value
	resp.NotFound = value == nil
	if err != nil {
		resp.Error = err.Error()
	}

	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.GetCf(),
				Key:   req.GetKey(),
				Value: req.GetValue(),
			},
		},
	})

	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.GetCf(),
				Key: req.GetKey(),
			},
		},
	})

	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	limit := req.GetLimit()
	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.GetStartKey())

	var kvs []*kvrpcpb.KvPair
	var i uint32

	for ; i < limit && iter.Valid(); i++ {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			resp.Error = err.Error()
			break
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})

		iter.Next()
	}

	resp.Kvs = kvs
	return resp, err
}
