package main

import (
    "crypto/tls"
    "encoding/json"
    "log"
    "net"
    "net/http"
    "sync"
    "time"

    "github.com/influxdata/influxdb/client/v2"
)

// Data structure for metrics
type Metrics struct {
    Temperature float64 `json:"temperature"`
    Humidity    float64 `json:"humidity"`
    Timestamp   time.Time `json:"timestamp"`
}

// Agent function to collect and send data
func agent(wg *sync.WaitGroup, serverAddr string) {
    defer wg.Done()
    for {
        metrics := Metrics{
            Temperature: 25.0, // Example data
            Humidity:    60.0, // Example data
            Timestamp:   time.Now(),
        }
        data, err := json.Marshal(metrics)
        if err != nil {
            log.Println("Error marshalling metrics:", err)
            continue
        }

        conn, err := tls.Dial("tcp", serverAddr, &tls.Config{
            InsecureSkipVerify: true,
        })
        if err != nil {
            log.Println("Error connecting to server:", err)
            continue
        }

        _, err = conn.Write(data)
        if err != nil {
            log.Println("Error sending data:", err)
        }
        conn.Close()

        time.Sleep(10 * time.Second) // Send data every 10 seconds
    }
}

// Central server to receive and store data
func server() {
    influxClient, err := client.NewHTTPClient(client.HTTPConfig{
        Addr: "http://localhost:8086",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer influxClient.Close()

    listener, err := tls.Listen("tcp", ":8081", &tls.Config{
        Certificates: []tls.Certificate{loadTLSCert()},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer listener.Close()

    var wg sync.WaitGroup

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Println("Error accepting connection:", err)
            continue
        }
        wg.Add(1)
        go handleConnection(conn, influxClient, &wg)
    }

    wg.Wait()
}

// Handle incoming connections and store data in InfluxDB
func handleConnection(conn net.Conn, influxClient client.Client, wg *sync.WaitGroup) {
    defer wg.Done()
    defer conn.Close()

    var metrics Metrics
    decoder := json.NewDecoder(conn)
    if err := decoder.Decode(&metrics); err != nil {
        log.Println("Error decoding metrics:", err)
        return
    }

    bp, err := client.NewBatchPoints(client.BatchPointsConfig{
        Database:  "construction",
        Precision: "s",
    })
    if err != nil {
        log.Println("Error creating batch points:", err)
        return
    }

    pt, err := client.NewPoint("metrics", nil, map[string]interface{}{
        "temperature": metrics.Temperature,
        "humidity":    metrics.Humidity,
    }, metrics.Timestamp)
    if err != nil {
        log.Println("Error creating point:", err)
        return
    }

    bp.AddPoint(pt)
    if err := influxClient.Write(bp); err != nil {
        log.Println("Error writing to InfluxDB:", err)
    }
}

// Load TLS certificate
func loadTLSCert() tls.Certificate {
    cert, err := tls.LoadX509KeyPair("server.crt", "server.key")
    if err != nil {
        log.Fatal("Error loading TLS certificate:", err)
    }
    return cert
}

func main() {
    var wg sync.WaitGroup

    // Start server
    go server()

    // Start agents
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go agent(&wg, "localhost:8081")
    }

    wg.Wait()
}
