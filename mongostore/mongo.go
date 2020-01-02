package mongostore

import (
	"context"
	"crypto/tls"
	"net"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient struct {
	client *mongo.Client
}

// TLSDialer struct of dialer
type TLSDialer struct {
	Address string
}

// DialContext connects via tls and returns the connection
func (t TLSDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return tls.Dial("tcp", address, &tls.Config{InsecureSkipVerify: true})
}

func (db *MongoClient) Dial(hostName, replicaset, user, password string) error {
	opt := db.setOptions(hostName, replicaset, user, password)

	client, err := mongo.Connect(context.Background(), opt)
	if err != nil {
		return errors.Wrap(err, "could not connect to mongodb")
	}

	db.client = client

	err = db.client.Ping(context.Background(), opt.ReadPreference)
	if err != nil {
		return errors.Wrap(err, "could not ping database")
	}

	return nil
}

func (db *MongoClient) setOptions(hostName, replicaset, user, password string) *options.ClientOptions {
	opt := options.Client().SetAuth(options.Credential{
		Username:   user,
		Password:   password,
		AuthSource: user,
	})

	hosts := strings.Split(hostName, ",")
	opt = opt.SetHosts(hosts)
	opt = opt.SetDialer(TLSDialer{Address: hosts[0]})
	opt = opt.SetReplicaSet("rs0")

	return opt
}

func (db *MongoClient) Ping() error {
	err := db.client.Ping(context.TODO(), nil)
	if err != nil {
		return errors.Wrap(err, "could not ping mongodb")
	}

	return nil
}
