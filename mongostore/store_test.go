package mongostore

import (
	"context"
	"os"
	"testing"

	"github.com/Nerzal/eventsource"
	"github.com/globalsign/mgo/bson"
)

func initDB() (Store, error) {
	client := &MongoClient{}
	hostName := os.Getenv("MONGODB_SERVER")
	replicaSet := os.Getenv("MONGODB_REPLICASET_NAME")
	user := os.Getenv("MONGODB_USERNAME")
	passwort := os.Getenv("MONGODB_PASSWORD")
	database := os.Getenv("MONGODB_DATABASE")

	err := client.Dial(hostName, replicaSet, user, passwort)
	if err != nil {
		return nil, err
	}

	store := NewStore("test_collection", database, client)
	return store, nil
}

func TestSave(t *testing.T) {
	store, err := initDB()
	if err != nil {
		t.Error("Failed to initialize database: ", err)
	}

	record := eventsource.Record{
		Version: 12,
		Data:    nil,
	}

	objectID := bson.NewObjectId().Hex()
	t.Run("Save V12", func(t *testing.T) {
		err = store.Save(context.Background(), objectID, record)
		if err != nil {
			t.Error("Failed to save record: ", err)
		}
	})

	record = eventsource.Record{
		Version: 13,
		Data:    nil,
	}

	t.Run("Update to V13", func(t *testing.T) {
		err = store.Save(context.Background(), objectID, record)
		if err != nil {
			t.Error("Failed to save updated record: ", err)
		}
	})

}

func TestLoad(t *testing.T) {
	store, err := initDB()
	if err != nil {
		t.Error("Failed to initialize database: ", err)
	}

	record := eventsource.Record{
		Version: 12,
		Data:    nil,
	}

	objectID := bson.NewObjectId().Hex()

	t.Run("Insert TestDocument", func(t *testing.T) {
		err = store.Save(context.Background(), objectID, record)
		if err != nil {
			t.Error("Failed to save record: ", err)
		}
	})

	t.Run("Test Load", func(t *testing.T) {
		history, err := store.Load(context.TODO(), objectID, 0, 99)
		if err != nil {
			t.Error("Failed to load history: ", err)
		}

		if len(history) == 0 {
			t.Error("history should not be empty")
		}
	})
}
