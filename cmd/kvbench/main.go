package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mc714/src/kvraft"
	"mc714/src/labrpc"
	"mc714/src/models"
	"mc714/src/porcupine"
)

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func main() {
	peersFlag := flag.String("peers", "localhost:9000,localhost:9001,localhost:9002", "comma-separated KV HTTP addresses")
	clients := flag.Int("clients", 4, "number of concurrent clients")
	opsPerClient := flag.Int("ops", 50, "operations per client")
	keys := flag.Int("keys", 5, "number of distinct keys")
	writeRatio := flag.Float64("write-ratio", 0.5, "fraction of Put/Append operations")
	seed := flag.Int64("seed", 42, "PRNG seed")
	checkLin := flag.Bool("linearizable", true, "verify linearizability with Porcupine")
	csvOut := flag.Bool("csv", false, "print single CSV summary line")
	dump := flag.Bool("dump", false, "print every operation (debug)")
	runID := flag.String("run-id", "", "key namespace for this run (default: unique per run)")
	flag.Parse()

	if *runID == "" {
		*runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	addrs := strings.Split(*peersFlag, ",")

	var (
		mu         sync.Mutex
		latencies  []float64
		operations []porcupine.Operation
		wg         sync.WaitGroup
	)

	startBench := time.Now()

	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ends := make([]labrpc.RPCClient, len(addrs))
			for i, addr := range addrs {
				ends[i] = kvraft.MakeHTTPEnd(strings.TrimSpace(addr), 2*time.Second)
			}
			ck := kvraft.MakeClerk(ends)
			localRng := rand.New(rand.NewSource(*seed + int64(clientID)*1000))

			for j := 0; j < *opsPerClient; j++ {
				key := *runID + "-" + strconv.Itoa(localRng.Intn(*keys))
				val := fmt.Sprintf("c%d-op%d;", clientID, j)
				callTs := time.Since(startBench).Nanoseconds()
				t0 := time.Now()

				var inp models.KvInput
				var out models.KvOutput

				r := localRng.Float64()
				if r < *writeRatio*0.8 {
					ck.Append(key, val)
					inp = models.KvInput{Op: 2, Key: key, Value: val}
				} else if r < *writeRatio {
					ck.Put(key, val)
					inp = models.KvInput{Op: 1, Key: key, Value: val}
				} else {
					v := ck.Get(key)
					inp = models.KvInput{Op: 0, Key: key}
					out = models.KvOutput{Value: v}
				}

				latMs := float64(time.Since(t0).Microseconds()) / 1000.0
				retTs := time.Since(startBench).Nanoseconds()
				if *dump {
					fmt.Printf("c%d op%d key=%s in=%q out=%q lat=%.2fms\n",
						clientID, j, key, inp.Value, out.Value, latMs)
				}

				mu.Lock()
				latencies = append(latencies, latMs)
				if *checkLin {
					operations = append(operations, porcupine.Operation{
						Input:    inp,
						Call:     callTs,
						Output:   out,
						Return:   retTs,
						ClientId: clientID,
					})
				}
				mu.Unlock()
			}
		}(c)
	}

	wg.Wait()
	elapsed := time.Since(startBench).Seconds()

	sort.Float64s(latencies)
	totalOps := len(latencies)
	throughput := float64(totalOps) / elapsed
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	linStatus := "skipped"
	if *checkLin {
		res := porcupine.CheckOperationsTimeout(models.KvModel, operations, 5*time.Second)
		switch res {
		case porcupine.Ok:
			linStatus = "ok"
		case porcupine.Illegal:
			log.Fatalf("LINEARIZABILITY VIOLATION detected!")
		case porcupine.Unknown:
			linStatus = "timeout"
		}
	}

	if *csvOut {
		fmt.Printf("%d,%d,%d,%d,%.3f,%.2f,%.3f,%.3f,%.3f,%s\n",
			len(addrs), *clients, totalOps, *seed, elapsed, throughput, p50, p95, p99, linStatus)
		return
	}

	fmt.Printf("nodes=%d clients=%d ops=%d seed=%d duration=%.2fs throughput=%.1f ops/s p50=%.2fms p95=%.2fms p99=%.2fms linearizable=%s\n",
		len(addrs), *clients, totalOps, *seed, elapsed, throughput, p50, p95, p99, linStatus)
}
