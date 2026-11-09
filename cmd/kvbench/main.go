package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
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

func makeEnds(addrs []string, timeout time.Duration) ([]*kvraft.HTTPEnd, []labrpc.RPCClient) {
	typed := make([]*kvraft.HTTPEnd, len(addrs))
	rpcEnds := make([]labrpc.RPCClient, len(addrs))
	for i, addr := range addrs {
		typed[i] = kvraft.MakeHTTPEnd(strings.TrimSpace(addr), timeout)
		rpcEnds[i] = typed[i]
	}
	return typed, rpcEnds
}

func sumStats(ends []*kvraft.HTTPEnd) kvraft.HTTPStats {
	var total kvraft.HTTPStats
	for _, e := range ends {
		s := e.Stats()
		total.Attempts += s.Attempts
		total.WrongLeader += s.WrongLeader
		total.Failed += s.Failed
	}
	return total
}

// probe issues one Put at a time, forever, logging start/end in absolute Unix
// nanos so experiment scripts can join the log with fault-injection timestamps.
// Clerk retries internally, so an op that spans an outage returns when the
// cluster is serving again.
func probe(addrs []string, logPath, runID string, ops int, interval time.Duration) {
	out := os.Stdout
	if logPath != "" {
		f, err := os.Create(logPath)
		if err != nil {
			log.Fatalf("probe log: %v", err)
		}
		defer f.Close()
		out = f
	}
	fmt.Fprintln(out, "start_ns,end_ns,lat_ms")
	_, rpcEnds := makeEnds(addrs, 2*time.Second)
	ck := kvraft.MakeClerk(rpcEnds)

	for i := 0; i < ops; i++ {
		start := time.Now()
		ck.Put(runID+"-probe", strconv.Itoa(i))
		end := time.Now()
		fmt.Fprintf(out, "%d,%d,%.3f\n", start.UnixNano(), end.UnixNano(),
			float64(end.Sub(start).Microseconds())/1000.0)
		time.Sleep(interval)
	}
}

func main() {
	peersFlag := flag.String("peers", "localhost:9000,localhost:9001,localhost:9002", "comma-separated KV HTTP addresses")
	clients := flag.Int("clients", 4, "number of concurrent clients")
	opsPerClient := flag.Int("ops", 50, "operations per client (probe: total writes)")
	keys := flag.Int("keys", 5, "number of distinct keys")
	writeRatio := flag.Float64("write-ratio", 0.5, "fraction of Put/Append operations")
	seed := flag.Int64("seed", 42, "PRNG seed")
	checkLin := flag.Bool("linearizable", true, "verify linearizability with Porcupine")
	csvOut := flag.Bool("csv", false, "print single CSV summary line")
	dump := flag.Bool("dump", false, "print every operation (debug)")
	runID := flag.String("run-id", "", "key namespace for this run (default: unique per run)")
	probeMode := flag.Bool("probe", false, "availability probe: one Put at a time, absolute timestamps")
	probeLog := flag.String("probe-log", "", "file for probe output (default stdout)")
	probeInterval := flag.Duration("probe-interval", 20*time.Millisecond, "pause between probe writes")
	flag.Parse()

	if *runID == "" {
		*runID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}

	addrs := strings.Split(*peersFlag, ",")
	if *probeMode {
		probe(addrs, *probeLog, *runID, *opsPerClient, *probeInterval)
		return
	}

	var (
		mu         sync.Mutex
		latencies  []float64
		attempts   []float64
		retriedOps int
		stats      kvraft.HTTPStats
		operations []porcupine.Operation
		wg         sync.WaitGroup
	)

	startBench := time.Now()

	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ends, rpcEnds := makeEnds(addrs, 2*time.Second)
			ck := kvraft.MakeClerk(rpcEnds)
			localRng := rand.New(rand.NewSource(*seed + int64(clientID)*1000))

			for j := 0; j < *opsPerClient; j++ {
				key := *runID + "-" + strconv.Itoa(localRng.Intn(*keys))
				val := fmt.Sprintf("c%d-op%d;", clientID, j)
				before := sumStats(ends)
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
				after := sumStats(ends)
				opAttempts := float64(after.Attempts - before.Attempts)
				if *dump {
					fmt.Printf("c%d op%d key=%s in=%q out=%q attempts=%.0f lat=%.2fms\n",
						clientID, j, key, inp.Value, out.Value, opAttempts, latMs)
				}

				mu.Lock()
				latencies = append(latencies, latMs)
				attempts = append(attempts, opAttempts)
				if opAttempts > 1 {
					retriedOps++
				}
				stats.Attempts += after.Attempts - before.Attempts
				stats.WrongLeader += after.WrongLeader - before.WrongLeader
				stats.Failed += after.Failed - before.Failed
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

	totalOps := len(latencies)
	throughput := float64(totalOps) / elapsed
	sort.Float64s(latencies)
	sort.Float64s(attempts)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	retryPct := 100 * float64(retriedOps) / float64(totalOps)
	attemptsPerOp := float64(stats.Attempts) / float64(totalOps)
	attemptsP99 := percentile(attempts, 0.99)
	wrongLeaderPct := 0.0
	if stats.Attempts > 0 {
		wrongLeaderPct = 100 * float64(stats.WrongLeader) / float64(stats.Attempts)
	}

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
		fmt.Printf("%d,%d,%d,%d,%.3f,%.2f,%.3f,%.3f,%.3f,%.2f,%.2f,%.1f,%.2f,%s\n",
			len(addrs), *clients, totalOps, *seed, elapsed, throughput, p50, p95, p99,
			retryPct, attemptsPerOp, attemptsP99, wrongLeaderPct, linStatus)
		return
	}

	fmt.Printf("nodes=%d clients=%d ops=%d seed=%d duration=%.2fs throughput=%.1f ops/s p50=%.2fms p95=%.2fms p99=%.2fms retry=%.2f%% attempts/op=%.2f wrong_leader=%.2f%% linearizable=%s\n",
		len(addrs), *clients, totalOps, *seed, elapsed, throughput, p50, p95, p99,
		retryPct, attemptsPerOp, wrongLeaderPct, linStatus)
}
