package mongostore

import (
	"context"
	"fmt"
	"github.com/Nerzal/eventsource"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Store is used as store
type Store interface {
	Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error
	Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (eventsource.History, error)
}

type store struct {
	collection string
	database   string
	db         *MongoClient
}

// NewStore instantiates a new Store
func NewStore(collection, database string, db *MongoClient) Store {
	return &store{
		collection: collection,
		database:   database,
		db:         db,
	}
}

type mongoRecord struct {
	AggregateID primitive.ObjectID   `bson:"_id"`
	Records     []eventsource.Record `bson:"records"`
}

func (store *store) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	objectID, err := primitive.ObjectIDFromHex(aggregateID)
	if err != nil {
		return errors.Wrap(err, "could not create objectID from provided hex")
	}

	databaseObject := mongoRecord{
		AggregateID: objectID,
		Records:     records,
	}

	collection := store.db.client.Database(store.database).Collection(store.collection)
	containsDocument, err := store.containsDocument(ctx, collection, objectID)
	if err != nil {
		return errors.Wrap(err, "could not test for existing document")
	}

	if containsDocument {
		return store.update(ctx, collection, &databaseObject)
	}

	return store.insert(ctx, collection, &databaseObject)
}

func (store *store) containsDocument(ctx context.Context, collection *mongo.Collection, aggregateID primitive.ObjectID) (bool, error) {
	filter := bson.D{{Key: "_id", Value: bson.M{"$eq": aggregateID}}}
	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return false, errors.Wrap(err, "could not count documents")
	}

	return count > 0, nil
}

func (store *store) update(ctx context.Context, collection *mongo.Collection, databaseObject *mongoRecord) error {
	filter := bson.D{{Key: "_id", Value: bson.M{"$eq": databaseObject.AggregateID}}}
	update := bson.D{{Key: "$set", Value: bson.M{"records": databaseObject.Records}}}
	_, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not update aggregate with id %s", databaseObject.AggregateID.Hex()))
	}

	return nil
}

func (store *store) insert(ctx context.Context, collection *mongo.Collection, databaseObject *mongoRecord) error {
	_, err := collection.InsertOne(ctx, databaseObject)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not insert aggregate with id %s", databaseObject.AggregateID.Hex()))
	}

	return nil
}

func (store *store) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (eventsource.History, error) {
	objectID, err := primitive.ObjectIDFromHex(aggregateID)
	if err != nil {
		return nil, errors.Wrap(err, "could not create objectID from provided hex")
	}

	collection := store.db.client.Database(store.database).Collection(store.collection)
	filter := bson.D{{Key: "_id", Value: bson.M{"$eq": objectID}}}

	result := collection.FindOne(ctx, filter)
	err = result.Err()
	if err != nil {
		return nil, err
	}

	var mongoRecord mongoRecord
	err = result.Decode(&mongoRecord)
	if err != nil {
		return nil, errors.Wrap(err, "could not decode record history")
	}

	var history []eventsource.Record
	for _, record := range mongoRecord.Records {
		if record.Version < fromVersion {
			continue
		}

		if record.Version > toVersion {
			continue
		}

		history = append(history, record)
	}

	return history, nil
}
