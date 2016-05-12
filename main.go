// Read from csv file

package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/cavaliercoder/grab"

	flag "github.com/ogier/pflag"

	pq "github.com/lib/pq"
)

func unzipFileIfNeeded(filename string) error {
	if strings.HasSuffix(filename, ".zip") {
		fmt.Printf("Unzipping %s...\033[K\n", path.Base(filename))
		out, err := exec.Command("unzip", "-d", fmt.Sprintf("./%s", path.Dir(filename)), filename).Output()
		if err != nil {
			return fmt.Errorf("%s\n%s\n", err, out)
		}
	}

	return nil
}

func downloadAllUrls(datasetName string, urls []string) ([]string, error) {
	var reqs []*grab.Request
	var responses []*grab.Response
	var resultFilenames []string

	for _, url := range urls {
		tokens := strings.Split(url, "/")
		downloadFilename := tokens[len(tokens)-1]
		resultFilename := downloadFilename

		if strings.HasSuffix(downloadFilename, ".zip") {
			resultFilename = strings.TrimSuffix(downloadFilename, ".zip")
		}

		resultPath := fmt.Sprintf("./downloads/%s/%s", datasetName, resultFilename)
		downloadPath := fmt.Sprintf("./downloads/%s/%s", datasetName, downloadFilename)

		resultFilenames = append(resultFilenames, resultPath)

		if _, err := os.Stat(resultPath); err == nil {
			log.Printf("File %s has already been downloaded, skipping", downloadFilename)
			continue
		}

		req, err := grab.NewRequest(url)
		req.Filename = downloadPath
		if err != nil {
			return nil, err
		}

		reqs = append(reqs, req)
	}

	fmt.Printf("Downloading %d files...\n", len(reqs))
	respch := grab.DefaultClient.DoBatch(3, reqs...)

	t := time.NewTicker(200 * time.Millisecond)

	completed := 0
	inProgress := 0
	for completed < len(reqs) {
		select {
		case resp := <-respch:
			if resp != nil {
				responses = append(responses, resp)
			}

		case <-t.C:
			if inProgress > 0 {
				fmt.Printf("\033[%dA\033[K", inProgress)
			}

			for i, resp := range responses {
				if resp != nil && resp.IsComplete() {
					if resp.Error != nil {
						fmt.Fprintf(os.Stderr, "Error downloading %s: %v\n", resp.Request.URL(), resp.Error)
					} else {
						fmt.Printf("Finished %s %d / %d bytes (%d%%)\n", resp.Filename, resp.BytesTransferred(), resp.Size, int(100*resp.Progress()))
					}

					err := unzipFileIfNeeded(resp.Filename)
					if err != nil {
						fmt.Printf("Failed to unzip %s: %s", resp.Filename, err)
					}

					responses[i] = nil
					completed++
				}
			}

			inProgress = 0
			for _, resp := range responses {
				if resp != nil {
					inProgress++
					fmt.Printf("Downloading %s %d / %d bytes (%d%%)\033[K\n", resp.Filename, resp.BytesTransferred(), resp.Size, int(100*resp.Progress()))
				}
			}
		}
	}

	t.Stop()

	return resultFilenames, nil
}

func createTable(db *sql.DB, fields []string, config datasetConfig) (string, error) {
	tableName := "pg_fancy_bench" //+ strings.Replace(uuid.NewV4().String(), "-", "_", -1)

	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return tableName, err
	}

	stmt := fmt.Sprintf("CREATE TABLE %s (", tableName)

	items := make([]string, 0, len(fields))
	for _, field := range fields {
		items = append(items, fmt.Sprintf("%s %s", field, "text"))
	}

	stmt += strings.Join(items, ",")
	stmt += ")"

	_, err = db.Exec(stmt)
	if err != nil {
		return tableName, err
	}

	_, err = db.Exec("SELECT master_create_distributed_table($1, $2, 'hash');", tableName, config.ShardKey)
	if err != nil {
		return tableName, err
	}

	_, err = db.Exec("SELECT master_create_worker_shards($1, $2, 1);", tableName, 16)
	if err != nil {
		return tableName, err
	}

	return tableName, nil
}

func insertMicroBatch(db *sql.DB, tableName string, batch [][]string, fields []string, wg *sync.WaitGroup) {
	defer wg.Done()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("Could not open transaction: %+v", err)
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(tableName, fields...))
	if err != nil {
		log.Fatalf("Could not prepare COPY: %+v", err)
		return
	}

	for _, data := range batch {
		args := make([]interface{}, len(data))
		for i, v := range data {
			args[i] = v
		}

		_, execErr := stmt.Exec(args...)
		if execErr != nil {
			log.Fatalf("Could not execute COPY line: %+v", execErr)
			return
		}
	}

	// SYNC with the error stream to make sure we covered all pending inserts
	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("Could not execute last COPY line: %+v", err)
		return
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("Could not close COPY: %+v", err)
		return
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("Could not commit transaction: %+v", err)
		return
	}
}

func chunkAndProcessMicroBatch(db *sql.DB, tableName string, microBatch [][]string, fields []string) {
	var wg sync.WaitGroup

	chunkSize := (microBatchSize / concurrency)

	for i := 0; i < len(microBatch); i += chunkSize {
		end := i + chunkSize
		if end > len(microBatch) {
			end = len(microBatch)
		}
		wg.Add(1)
		go insertMicroBatch(db, tableName, microBatch[i:end], fields, &wg)
	}
	wg.Wait()
}

const microBatchSize = 100000
const concurrency = 2

type datasetConfig struct {
	DownloadURLs []string
	ShardKey     string
	Queries      []string
}

func main() {
	var config datasetConfig
	var databaseURL string
	var datasetName string

	flag.StringVarP(&databaseURL, "database-url", "d", "", "Specifies the postgresql:// database URL we should connect to")
	flag.StringVarP(&datasetName, "dataset", "s", "", "Specifies the dataset that should be used")
	flag.Parse()

	if datasetName == "" {
		log.Fatalf("Error! You need to specify a dataset name using the -s option")
		return
	}

	if _, err := toml.DecodeFile(fmt.Sprintf("./datasets/%s.toml", datasetName), &config); err != nil {
		log.Fatalf("Could not load dataset config: %s\n", err)
		return
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		log.Fatalf("Could not connect to database: %s\n", err)
		return
	}

	db.SetMaxOpenConns(concurrency)

	err = db.Ping()
	if err != nil {
		log.Fatalf("Could not connect to database: %s\n", err)
		return
	}

	fileNames, err := downloadAllUrls(datasetName, config.DownloadURLs)
	if err != nil {
		log.Fatal(err)
		return
	}

	n := 0
	fmt.Printf("Starting up, will show insert rate every 10 seconds\n")

	go func() {
		for {
			prevN := n
			time.Sleep(10 * time.Second)

			fmt.Printf("\nInserting at a rate of %0.1fk records / second\n", float32(n-prevN)/1000.0/10)
		}
	}()

	tableCreated := false
	tableName := ""

	for _, filename := range fileNames {
		f, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		reader := csv.NewReader(bufio.NewReader(f))

		fields, err := reader.Read()
		if err != nil {
			log.Fatalf("Could not read CSV header: %s\n", err)
			return
		}

		paramRefs := make([]string, 0, len(fields))
		for idx := range fields {
			paramRefs = append(paramRefs, fmt.Sprintf("$%d", idx+1))
		}

		if !tableCreated {
			tableName, err = createTable(db, fields, config)
			if err != nil {
				log.Fatalf("Could not create table: %s\n", err)
				return
			}
			tableCreated = true
		}

		microBatch := make([][]string, 0, microBatchSize)
		for {
			record, err := reader.Read()
			if err == io.EOF {
				// Process all remaining data
				chunkAndProcessMicroBatch(db, tableName, microBatch, fields)
				break
			}
			if err != nil {
				log.Fatal(err)
				return
			}

			microBatch = append(microBatch, record)

			if n != 0 && n%microBatchSize == 0 {
				chunkAndProcessMicroBatch(db, tableName, microBatch, fields)
				microBatch = microBatch[:0]

				fmt.Print(".")
			}

			n++
		}
	}

	log.Print("Done\n")
}
