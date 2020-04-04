package connector

import (
	"context"
	"fmt"
	"crypto/tls"

	godriver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type ArangodbConnector struct {
	ArangoDbUrls string
	CollectionName string
	Collection godriver.Collection
	Client godriver.Client
	Connection godriver.Connection
	Database godriver.Database
	DatabaseName string
	UserName string
	UserPassword string
}

var (
	ctx context.Context = context.Background()
)

func(c *ArangodbConnector) Connect() error {
	var err error
	c.Connection, err = http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{c.ArangoDbUrls},
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
	})
	return err
}

func(c *ArangodbConnector) NewClient() error {
	var err error
	clientConfig := godriver.ClientConfig{
		Connection: c.Connection,
		Authentication: godriver.BasicAuthentication(c.UserName, c.UserPassword),
	}
	c.Client, err = godriver.NewClient(clientConfig)
	return err
}

func(c *ArangodbConnector) Close () error {
	return nil
}

func(c *ArangodbConnector) DatabaseExists() (*bool, error) {
	var (
		dbExist bool
		err error
	)
	if c.Client == nil {
		return nil, fmt.Errorf("ArangoDB client has not yet opened")
	}
	dbExist, err = c.Client.DatabaseExists(ctx, c.DatabaseName) 
	if err != nil {
		return nil, err
	}
	return &dbExist, nil
}

func(c *ArangodbConnector) OpenDatabase() error {
	var err error
	if c.Client == nil {
		return fmt.Errorf("ArangoDB client has not yet opened")
	}
	c.Database, err = c.Client.Database(ctx, c.DatabaseName)
	return err
}

func(c *ArangodbConnector) CollectionExists() (*bool, error) {
	var (
		colExist bool
		err error
	)
	if c.Database == nil {
		return nil, fmt.Errorf("Database %v has not yet opened", c.DatabaseName)
	}
	colExist, err = c.Database.CollectionExists(ctx, c.CollectionName)
	if err != nil {
		return nil, err
	}
	return &colExist, nil

}

func(c *ArangodbConnector) OpenCollection() error {
	var err error
	if c.Database == nil {
		return fmt.Errorf("Database %v has not yet opened", c.DatabaseName)
	}
	c.Collection, err = c.Database.Collection(ctx, c.CollectionName)
	return err
}

func(c *ArangodbConnector) CreateDocument(doc interface{}) (*godriver.DocumentMeta, error) {
	if c.Database == nil {
		return nil, fmt.Errorf("Database %v has not yet opened", c.DatabaseName)
	}
	if c.Collection == nil {
		return nil, fmt.Errorf("Collection %v has not yet opened", c.CollectionName)
	}
	meta, err := c.Collection.CreateDocument(ctx, doc)
	if err != nil {
		return nil, err
	}
	return &meta, nil

}

func(c *ArangodbConnector) UpdateDocument(key string, doc interface{}) (*godriver.DocumentMeta, error) {
	if c.Database == nil {
		return nil, fmt.Errorf("Database %v has not yet opened", c.DatabaseName)
	}
	if c.Collection == nil {
		return nil, fmt.Errorf("Collection %v has not yet opened", c.CollectionName)
	}
	meta, err := c.Collection.UpdateDocument(ctx, key, doc)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func(c *ArangodbConnector) DeleteDocument(key string) (*godriver.DocumentMeta, error) {
	if c.Database == nil {
		return nil, fmt.Errorf("Database %v has not yet opened", c.DatabaseName)
	}
	if c.Collection == nil {
		return nil, fmt.Errorf("Collection %v has not yet opened", c.CollectionName)
	}
	meta, err := c.Collection.RemoveDocument(ctx, key)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}
