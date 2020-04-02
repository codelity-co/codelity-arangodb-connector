package connector

import (
	"context"

	godriver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type ArangoDbConnector struct {
	ArangoDbUrls string
	CollectionName string
	Collection godriver.Collection
	Client godriver.Client
	Connection godriver.Connection
	Database godriver.Database
	DatabaseName string
}

var (
	ctx context.Context = context.Background()
)

func(c *ArangoDbConnector) Connect() error {
	var err error

	c.Connection, err = http.NewConnection(http.ConnectionConfig{
    Endpoints: []string{c.ArangoDbUrls},
	})
	if err != nil {
		return err
	}

	clientConfig := godriver.ClientConfig{
		Connection: c.Connection,
	}
	c.Client, err = godriver.NewClient(clientConfig)
	if err != nil {
		return err
	}

	var dbExist bool
	dbExist, err = c.Client.DatabaseExists(ctx, c.DatabaseName) 
	if err != nil {
		return err
	}
	if !dbExist {
		createDbOptions := &godriver.CreateDatabaseOptions{}
		c.Database, err = c.Client.CreateDatabase(ctx, c.DatabaseName, createDbOptions)
		if err != nil {
			return err
		}
	} else {
		c.Database, err = c.Client.Database(ctx, c.DatabaseName)
		if err != nil {
			return err
		}
	}

	var colExist bool
	colExist, err = c.Database.CollectionExists(ctx, c.CollectionName)
	if err != nil {
		return err
	}

	if !colExist {
		createColOptions := &godriver.CreateCollectionOptions{}
		c.Collection, err = c.Database.CreateCollection(ctx, c.CollectionName, createColOptions)
		if err != nil {
			return err
		}
	} else {
		c.Collection, err = c.Database.Collection(ctx, c.CollectionName)
		if err != nil {
			return err
		}
	}

	return nil
}

func(c *ArangoDbConnector) CreateDocument(doc interface{}) (*godriver.DocumentMeta, error) {
	meta, err := c.Collection.CreateDocument(ctx, doc)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func(c *ArangoDbConnector) UpdateDocument(key string, doc interface{}) (*godriver.DocumentMeta, error) {
	meta, err := c.Collection.UpdateDocument(ctx, key, doc)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func(c *ArangoDbConnector) DeleteDocument(key string) (*godriver.DocumentMeta, error) {
	meta, err := c.Collection.RemoveDocument(ctx, key)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}
