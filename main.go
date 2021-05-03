package main

import (
	"context"
	"fmt"
	"log"

	schema "github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
)

var client immuclient.ImmuClient
var ctx context.Context

func main() {
	key := []byte(`json1`)
	value := []byte(`{
		"key1": "value1", 
		"obj1": {
			"keyx": "value xy"
		},
		"num1": 123
	}`)

	connect()
	selectDb()

	if setVal(key, value, true) {
		chk, result := getVal(key, true)

		if chk {
			fmt.Println("Result of query: %s\n", result)
		} else {
			fmt.Println("Cannot get result of query")
		}
	}
}

func getVal(key []byte, verified bool) (bool, string) {
	var entry *schema.Entry
	var err error = nil

	if verified {
		entry, err = client.VerifiedGet(ctx, key)
	} else {
		entry, err = client.Get(ctx, key)
	}

	if err != nil {
		log.Fatal("Error cannot read data: ", err)
		return false, ""
	}

	return true, "Key: " + string(entry.Key) +
		", Value: " + string(entry.Value) +
		", Tx: " + string(entry.Tx)
}

func setVal(key []byte, value []byte, verified bool) bool {
	var tx *schema.TxMetadata
	var err error = nil

	if verified {
		tx, err = client.VerifiedSet(ctx, key, value)
	} else {
		tx, err = client.Set(ctx, key, value)
	}

	if err != nil {
		log.Fatal("Error cannot set data: ", err)
		return false
	}
	fmt.Printf("Set data, with key: %s, value %s and tx %d\n", key, value, tx.Id)

	return true
}

func selectDb() {
	resp, err := client.UseDatabase(ctx, &schema.Database{
		Databasename: "testing1",
	})
	if err != nil {
		log.Fatal("Error on selecting db: ", err)
	}
	fmt.Printf("auth token (selecting db): %v\n", resp.Token)
	md := metadata.Pairs("authorization", resp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)
}

func connect() {
	var err error = nil
	client, err = immuclient.NewImmuClient(immuclient.DefaultOptions().WithDatabase("testing1"))
	if err != nil {
		log.Fatal("Error on creating client, ", err)
	}

	ctx = context.Background()

	// Login                                 user             password
	login, err := client.Login(ctx, []byte(`usertest1`), []byte(`UsTest01.`))
	if err != nil {
		log.Fatal("Error on connecting, ", err)
	}
	fmt.Printf("auth token (login): %v\n", login.Token)

	fmt.Println("Database selected: ", client.GetOptions().Database)

	// Set auth in context for future operations
	md := metadata.Pairs("authorization", login.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)
}
