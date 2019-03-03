package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/protobuf/proto"

	"github.com/pailakka/mqttgtfsrtbatcher/transit_realtime"
)

var tdbytes = []byte("TripDescriptor")
var gtfsrtversion = "2.0"

func calcTripDescriptorHash(td *transit_realtime.TripDescriptor) uint64 {
	hash := fnv.New64a()

	diridb := make([]byte, 4)
	binary.LittleEndian.PutUint32(diridb, td.GetDirectionId())
	hash.Write(tdbytes)
	hash.Write([]byte(td.GetTripId()))
	hash.Write([]byte(td.GetRouteId()))
	hash.Write(diridb)
	hash.Write([]byte(td.GetStartTime()))
	hash.Write([]byte(td.GetStartDate()))
	hash.Write([]byte(td.GetScheduleRelationship().String()))
	return hash.Sum64()
}

func calcFeedEntityHash(entity *transit_realtime.FeedEntity) uint64 {
	if entity.TripUpdate != nil {
		return calcTripDescriptorHash(entity.TripUpdate.GetTrip())
	}

	return 0
}

type GTFSRTBatch struct {
	GTFSRTBatch map[uint64]*transit_realtime.FeedEntity
	GTFSRTInput chan *GTFSRTTimestampedEntity
}

type GTFSRTTimestampedEntity struct {
	Hash      uint64
	Timestamp uint64
	Entity    *transit_realtime.FeedEntity
	Size      int
}

func (gtfsrtbatcher *GTFSRTBatch) Init(quit <-chan bool, sendBatch func(payload MQTTPayload)) {
	gtfsrtbatcher.GTFSRTInput = make(chan *GTFSRTTimestampedEntity, 1000)
	gtfsrtbatcher.GTFSRTBatch = make(map[uint64]*transit_realtime.FeedEntity)
	batchSendTimer := time.NewTicker(200 * time.Millisecond)

	go func() {
		var messageCounter uint64
		var messageSize uint64
		for {
			select {
			case <-batchSendTimer.C:
				if messageCounter > 0 {
					fmt.Println(time.Now(), "Send batch!")
					i := 0
					var outfm = &transit_realtime.FeedMessage{}
					now := uint64(time.Now().Unix())
					outfm.Header = &transit_realtime.FeedHeader{
						GtfsRealtimeVersion: &gtfsrtversion,
						Incrementality:      transit_realtime.FeedHeader_DIFFERENTIAL.Enum(),
						Timestamp:           &now,
					}
					outfm.Entity = make([]*transit_realtime.FeedEntity, len(gtfsrtbatcher.GTFSRTBatch))
					for _, v := range gtfsrtbatcher.GTFSRTBatch {
						outfm.Entity[i] = v
						i++
					}

					go func() {
						out, err := proto.Marshal(outfm)
						if err != nil {
							log.Fatalln("Failed to encode batched feedMessage:", err)
						}
						sendBatch(MQTTPayload{Topic: "gtfsrt/batch/fi/hsl/tu", Payload: out})
					}()

					if i > 0 {
						fmt.Println(i, messageCounter, "items in batch.", messageSize, "bytes")
					}
					gtfsrtbatcher.GTFSRTBatch = make(map[uint64]*transit_realtime.FeedEntity)
					messageCounter = 0
					messageSize = 0
				}
			case entity := <-gtfsrtbatcher.GTFSRTInput:
				gtfsrtbatcher.GTFSRTBatch[entity.Hash] = entity.Entity
				messageCounter++
				messageSize += uint64(entity.Size)

			case <-quit:
				fmt.Println("Quit batcher!")
				return
			}
		}
	}()
}

func (gtfsrtbatcher *GTFSRTBatch) MessageHandler(client mqtt.Client, msg mqtt.Message) {
	origfeedmessage := &transit_realtime.FeedMessage{}
	if err := proto.Unmarshal(msg.Payload(), origfeedmessage); err != nil {
		log.Fatalln("Failed to parse GTFSRT FeedMessage:", err)
	}
	for _, entity := range origfeedmessage.Entity {
		gtfsrtbatcher.GTFSRTInput <- &GTFSRTTimestampedEntity{Hash: calcFeedEntityHash(entity), Timestamp: origfeedmessage.GetHeader().GetTimestamp(), Entity: entity, Size: len(msg.Payload())}
	}
}
