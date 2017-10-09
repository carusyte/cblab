package main

import (
	"fmt"
	"time"
	"gopkg.in/couchbase/gocb.v1"
	"log"
	"strconv"
	"math/rand"
)

func main() {
	bulkMutation()
}

func bulkMutation() {
	b := Cb()
	defer b.Close()
	createMap(b, "MAP", 1000)
	mib := b.MutateIn("MAP", 0, 0)
	for i := 0; i < 990; i++ {
		mib.Remove(strconv.Itoa(i))
		if (i != 0 && i%15 == 0) || i == 990-1 {
			_, e := mib.Execute()
			if e != nil {
				log.Printf("error at %d \n %+v", i, e)
			}
			mib = b.MutateIn("MAP", 0, 0)
		}
	}
	log.Println("bulkMutation() complete")
}

func createMap(bucket *gocb.Bucket, key string, size int) {
	m := make(map[string][]int)
	for i := 0; i < size; i++ {
		slice := make([]int, rand.Intn(10)+5)
		for j := 0; j < len(slice); j++ {
			slice[j] = rand.Intn(1000)
		}
		m[strconv.Itoa(i)] = slice
	}
	c, e := bucket.Upsert(key, m, 0)
	log.Printf("\"%s\" map[%d] injected, cas=%+v, e=%+v", key, len(m), c, e)
}

func Cb() *gocb.Bucket {
	cbclus, e := gocb.Connect(fmt.Sprintf("couchbase://10.16.53.10,10.16.53.11"))
	if e != nil {
		log.Panicln("failed to connect to couchbase cluster.", e)
	}
	cbclus.SetEnhancedErrors(true)
	bucket, e := cbclus.OpenBucket("default", "")
	if e != nil {
		log.Panicln("failed to open couchbase bucket", e)
	}
	timeout := time.Second * 30
	bucket.SetOperationTimeout(timeout)
	bucket.SetDurabilityTimeout(timeout)
	bucket.SetViewTimeout(timeout)
	bucket.SetBulkOperationTimeout(timeout)
	bucket.SetDurabilityPollTimeout(timeout)
	bucket.SetN1qlTimeout(timeout)
	return bucket
}
