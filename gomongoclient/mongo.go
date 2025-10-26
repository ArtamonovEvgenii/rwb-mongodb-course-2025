package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const businessTripsCollectionName = "business_trips"

type Mongo struct {
	client               *mongo.Database
	lgr                  *slog.Logger
	stocksCollectionName string
}

func NewMongo(
	ctx context.Context,
	mongoURI string,
	dbName string,
	stocksCollectionName string,
	lgr *slog.Logger,
) (*Mongo, error) {
	lgr = lgr.With("component", "mongodb-client")

	clientOptions := options.Client().ApplyURI(mongoURI)
	clientOptions.SetWriteConcern(writeconcern.Journaled())
	clientOptions.SetWriteConcern(writeconcern.Majority())

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = client.Ping(pingCtx, nil)
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	mongoClient := &Mongo{
		client:               client.Database(dbName),
		lgr:                  lgr,
		stocksCollectionName: stocksCollectionName,
	}

	return mongoClient, nil
}

func (m *Mongo) Close() {
	closeCtx, closeCtxCancel := context.WithTimeout(context.Background(), time.Second*15)
	defer closeCtxCancel()

	if err := m.client.Client().Disconnect(closeCtx); err != nil {
		m.lgr.Error("close mongodb connection", slog.String("error", err.Error()))
		return
	}

	m.lgr.Info("mongodb connection closed")
}

func (m *Mongo) FindHighStock(ctx context.Context, high float64) (stockSymbol string, err error) {
	stocksCollection := m.client.Collection(m.stocksCollectionName)

	filter := bson.D{{Key: "high", Value: high}}

	findOpts := &options.FindOneOptions{}
	findOpts.SetSort(bson.D{{Key: "stock_symbol", Value: 1}})

	res := stocksCollection.FindOne(ctx, filter, findOpts)
	if err = res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}

		return "", fmt.Errorf("find one: %w", err)
	}

	var stock stockPosition
	err = res.Decode(&stock)
	if err != nil {
		return "", fmt.Errorf("decode result: %w", err)
	}

	return stock.StockSymbol, nil
}

type stockPosition struct {
	Exchange    string  `bson:"exchange"`
	StockSymbol string  `bson:"stock_symbol"`
	Date        string  `bson:"date"`
	Open        float64 `bson:"open"`
	High        float64 `bson:"high"`
	Low         float64 `bson:"low"`
	CloseValue  float64 `bson:"close"`
	Volume      int     `bson:"volume"`
	AdjClose    float64 `bson:"adj close"`
}

//{
//_id: ObjectId('4d094f58c96767d7a0099d49'),
//exchange: 'NASDAQ',
//stock_symbol: 'AACC',
//date: '2008-03-07',
//open: 8.4,
//high: 8.75,
//low: 8.08,
//close: 8.55,
//volume: 275800,
//'adj close': 8.55
//}

//type businessTrip struct {
//	ID                  int64     `bson:"id"`
//	UUID                string    `bson:"uuid"`
//	OrderType           string    `bson:"orderType"`
//	DateFrom            time.Time `bson:"dateFrom"`
//	DateTo              time.Time `bson:"dateTo"`
//	StartLocationCode   string    `bson:"startLocationCode"`
//	EndLocationCode     string    `bson:"EndLocationCode"`
//	CreatedAt           time.Time `bson:"createdAt"`
//	UpdatedAt           time.Time `bson:"updatedAt"`
//	Creator             *employee `bson:"creator"`
//	Approver            *employee `bson:"approver"`
//	TravelManager       *employee `bson:"travelManager"`
//	ApprovedAt          time.Time `bson:"approvedAt"`
//	ApprovedByManagerAt time.Time `bson:"approvedByManagerAt"`
//	TotalPrice          int       `bson:"totalPrice"`
//	Status              string    `bson:"status"`
//	FlightOrdersUUIDs   []string  `bson:"flightOrdersUUIDs"`
//	TimeLimitAt         string    `bson:"timeLimitAt"`
//	BusinessTripType    string    `bson:"businessTripType"`
//	HotelOrderUUIDs     []string  `bson:"hotelOrdersUUIDs,omitempty"`
//}

/*
{
  _id: ObjectId('4d094f58c96767d7a0099d49'),
  exchange: 'NASDAQ',
  stock_symbol: 'AACC',
  date: '2008-03-07',
  open: 8.4,
  high: 8.75,
  low: 8.08,
  close: 8.55,
  volume: 275800,
  'adj close': 8.55
}
*/

//func (m *Mongo) UpdateOrderType(ctx context.Context, dryRun bool) error {
//	btripCollection := m.client.Collection(businessTripsCollectionName)
//
//	docCount, err := btripCollection.CountDocuments(ctx, bson.M{})
//	if err != nil {
//		return fmt.Errorf("count documents: %w", err)
//	}
//	m.lgr.Info("count documents", slog.Int64("count", docCount))
//
//	lastObjectID := "000000000000000000000000"
//
//	batchSize := int64(50)
//	totalResults := TotalResults{}
//	for {
//		var batchResult BatchResult
//		batchResult, err = m.processBatch(ctx, batchSize, lastObjectID, dryRun)
//		if err != nil {
//			return fmt.Errorf("process batch: %w", err)
//		}
//
//		totalResults.AppendBatchResult(batchResult)
//
//		lastObjectID = batchResult.lastObjectID
//
//		if batchResult.processedDocuments < batchSize {
//			break
//		}
//	}
//
//	m.lgr.Info("total results",
//		slog.Int("batches", totalResults.totalBatches),
//		slog.Int64("docs", totalResults.processedDocuments),
//		slog.Int("orderTypeUndefinedCount", totalResults.orderTypeUndefinedCount),
//		slog.Int("orderTypeMixedCount", totalResults.orderTypeMixedCount),
//		slog.Int("orderTypeFlightCount", totalResults.orderTypeFlightCount),
//		slog.Int("orderTypeHotelCount", totalResults.orderTypeHotelCount),
//		slog.Int64("updatedCount", totalResults.updatedCount),
//	)
//
//	return nil
//}
//
//func (m *Mongo) processBatch(
//	ctx context.Context,
//	batchSize int64,
//	lastObjectID string,
//	dryRun bool,
//) (BatchResult, error) {
//	batchResult := BatchResult{}
//
//	btripCollection := m.client.Collection(businessTripsCollectionName)
//
//	findOpts := &options.FindOptions{}
//	findOpts.SetLimit(batchSize).SetSort(bson.D{{Key: "_id", Value: 1}})
//
//	objectID, err := primitive.ObjectIDFromHex(lastObjectID)
//	if err != nil {
//		return batchResult, fmt.Errorf("create object ID from hex: %w", err)
//	}
//	filter := bson.M{"_id": bson.M{"$gt": objectID}}
//
//	cur, err := btripCollection.Find(ctx, filter, findOpts)
//	if err != nil {
//		return batchResult, fmt.Errorf("find: %w", err)
//	}
//
//	models := make([]mongo.WriteModel, 0, batchSize)
//
//	for cur.Next(ctx) {
//		var businessTripDTO businessTrip
//		err = cur.Decode(&businessTripDTO)
//		if err != nil {
//			return batchResult, fmt.Errorf("decode: %w", err)
//		}
//
//		batchResult.IncProcessedDocuments()
//		batchResult.SetLastObjectID(businessTripDTO.ObjectID.Hex())
//
//		var orderType OrderType
//		var orderTypeExist bool
//		orderType, orderTypeExist, err = businessTripDTO.GetOrderType()
//		if err != nil {
//			m.lgr.Error("get order type",
//				slog.String("uuid", businessTripDTO.UUID),
//				slog.String("error", err.Error()))
//			continue
//		}
//
//		if orderType == OrderTypeUndefined || orderType == OrderTypeMixed {
//			m.lgr.Debug("suspicious order type",
//				slog.String("orderType", orderType.String()),
//				slog.Bool("orderTypeExist", orderTypeExist),
//				slog.String("uuid", businessTripDTO.UUID))
//			continue
//		}
//
//		batchResult.IncOrderType(orderType)
//
//		updateModel := mongo.NewUpdateOneModel().
//			SetFilter(bson.M{"uuid": businessTripDTO.UUID}).
//			SetUpdate(bson.M{"$set": bson.M{"orderType": orderType.String()}}).
//			SetUpsert(false)
//		models = append(models, updateModel)
//	}
//
//	if !dryRun {
//		writeOpts := options.BulkWrite().SetOrdered(false)
//
//		bulkWriteRes, err := btripCollection.BulkWrite(ctx, models, writeOpts)
//		if err != nil {
//			return batchResult, fmt.Errorf("bulk write: %w", err)
//		}
//
//		batchResult.IncUpdated(bulkWriteRes.ModifiedCount)
//	}
//
//	return batchResult, nil
//}
