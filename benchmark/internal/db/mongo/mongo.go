package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongo struct {
	client *mongo.Client
	db     *mongo.Database
}

func New(uri string, name string) (*Mongo, error) {

	client, err := mongo.Connect(context.Background(),
		options.Client().ApplyURI(uri))

	if err != nil {
		return nil, err
	}

	return &Mongo{
		client: client,
		db:     client.Database(name),
	}, nil
}

func (m *Mongo) Close() {
	m.client.Disconnect(context.Background())
}

func (m *Mongo) GetOrder(id int64, table string) error {

	return m.db.Collection("orders").
		FindOne(context.Background(),
			bson.M{"orderId": id}).Err()
}

func (m *Mongo) BuyProduct(id int64) (bool, error) {

	res := m.db.Collection("products").FindOneAndUpdate(
		context.Background(),
		bson.M{"productId": id, "stock": bson.M{"$gt": 0}},
		bson.M{"$inc": bson.M{"stock": -1}},
	)

	if res.Err() != nil {
		return false, nil
	}

	return true, nil
}

func (m *Mongo) ResetStock(id int64, q int) error {

	_, err := m.db.Collection("products").UpdateOne(
		context.Background(),
		bson.M{"productId": id},
		bson.M{"$set": bson.M{"stock": q}},
	)

	return err
}

func (m *Mongo) GetStock(id int64) (int, error) {

	var doc struct {
		Stock int `bson:"stock"`
	}

	err := m.db.Collection("products").
		FindOne(context.Background(), bson.M{"productId": id}).Decode(&doc)

	return doc.Stock, err
}

func (m *Mongo) RevenueByMonth(table string) error {

	pipeline := mongo.Pipeline{
		{{"$group", bson.M{
			"_id": bson.M{
				"year":  bson.M{"$year": "$orderDate"},
				"month": bson.M{"$month": "$orderDate"},
			},
			"revenue": bson.M{"$sum": "$totalAmount"},
		}}},
	}

	_, err := m.db.Collection("orders").Aggregate(context.Background(), pipeline)

	return err
}

func (m *Mongo) ExplainRevenue(table string) (string, error) {

	ctx := context.Background()

	pipeline := mongo.Pipeline{
		{{"$group", bson.M{
			"_id": bson.M{
				"year":  bson.M{"$year": "$orderDate"},
				"month": bson.M{"$month": "$orderDate"},
			},
			"revenue": bson.M{"$sum": "$totalAmount"},
		}}},
	}

	cmd := bson.D{
		{"explain", bson.D{
			{"aggregate", "orders"},
			{"pipeline", pipeline},
			{"cursor", bson.D{}},
		}},
		{"verbosity", "executionStats"},
	}

	var result bson.M

	err := m.db.RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return "", err
	}

	bytes, err := bson.MarshalExtJSON(result, true, true)
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (m *Mongo) CreateIndexes() error {

	ctx := context.Background()

	orders := m.db.Collection("orders")

	_, err := orders.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "orderId", Value: 1}},
			Options: options.Index().SetName("order_id_idx"),
		},
		{
			Keys: bson.D{{Key: "orderDate", Value: 1}},
			Options: options.Index().SetName("order_date_idx"),
		},
		{
			Keys: bson.D{{Key: "user.userId", Value: 1}},
			Options: options.Index().SetName("user_id_idx"),
		},
	})

	if err != nil {
		return err
	}

	products := m.db.Collection("products")

	_, err = products.Indexes().CreateOne(ctx,
		mongo.IndexModel{
			Keys: bson.D{{Key: "productId", Value: 1}},
			Options: options.Index().SetName("product_id_idx"),
		})

	if err != nil {
		return err
	}

	users := m.db.Collection("users")

	_, err = users.Indexes().CreateOne(ctx,
		mongo.IndexModel{
			Keys: bson.D{{Key: "userId", Value: 1}},
			Options: options.Index().SetName("user_id_idx"),
		})

	return err
}

func (m *Mongo) DropIndexes() error {

	ctx := context.Background()

	orders := m.db.Collection("orders")
	products := m.db.Collection("products")
	users := m.db.Collection("users")

	_, _ = orders.Indexes().DropOne(ctx, "order_id_idx")
	_, _ = orders.Indexes().DropOne(ctx, "order_date_idx")
	_, _ = orders.Indexes().DropOne(ctx, "user_id_idx")

	_, _ = products.Indexes().DropOne(ctx, "product_id_idx")

	_, _ = users.Indexes().DropOne(ctx, "user_id_idx")

	return nil
}