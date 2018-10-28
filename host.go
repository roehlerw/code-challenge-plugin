package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/naveego/code-challenge-plugin/plugin"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"io"
	golog "log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

var pluginStartupTimeout = 5 * time.Second
var log *golog.Logger
var flog *golog.Logger

func init() {
	log = golog.New(os.Stdout, color.BlueString("HOST  |"), golog.Ltime|golog.Lmicroseconds)
	file, _ := os.OpenFile(".log", os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0660)
	flog = golog.New(file, "", 0)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("expected at least one argument, the command to start the plugin (and its arguments, if any)")
	}

	stdoutReader, stdouthWriter := io.Pipe()
	cmd := exec.Command(os.Args[1], os.Args[2:]...)

	cmd.Stderr = os.Stdout
	cmd.Stdout = stdouthWriter

	exitCh := make(chan int)
	portCh := make(chan int)

	go monitorStdout(stdoutReader, portCh)

	if err := cmd.Start(); err != nil {
		log.Fatalf("couldn't start plugin: %s", err)
	}

	go monitorExit(cmd, exitCh)
	go handleUserExit(cmd)

	select {
	case <-time.After(pluginStartupTimeout):
		log.Fatalf("did not get a port from the plugin witlog.Printfn timeout of %s", pluginStartupTimeout)
	case exitCode := <-exitCh:
		if exitCode != 0 {
			log.Fatalf("plugin exited with non-zero code %d", exitCode)
		}
		os.Exit(exitCode)
	case port := <-portCh:
		err := runTests(port)
		log.Println("see .log file for all data processed")
		if err != nil {
			log.Fatalf("tests failed: %s", err)
		}
		log.Printf("done")
	}
}

func runTests(port int) error {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return errors.WithMessage(err, "connection failed")
	}

	client := plugin.NewPluginClient(conn)

	pwd, _ := os.Getwd()
	tests := []*testCase{
		{
			name:            "animals",
			glob:            filepath.Join(pwd, "./data/animals.csv"),
			expectedCount:   100,
			publishSchema:   schemaAnimals,
			expectedSchemas: []plugin.Schema{schemaAnimals},
			description:     "schema has multiple data types",
			expectedRecords: []*expectedRecord{
				{1, "Macropus fuliginosus", true, nil},
				{1, "Ciconia ciconia", false, nil},
			},
		},
		{
			name:            "logs",
			glob:            filepath.Join(pwd, "./data/*.csv"),
			expectedCount:   300,
			publishSchema:   schemaLogs,
			expectedSchemas: []plugin.Schema{schemaAnimals, schemaLogs, schemaPeople},
			description:     "schemas should be identified from headers",
		},
		{
			name:            "people",
			glob:            filepath.Join(pwd, "./data/people.*.csv"),
			expectedCount:   3000,
			publishSchema:   schemaPeople,
			expectedSchemas: []plugin.Schema{schemaLogs, schemaPeople},
			description:     "plugin should publish quickly",
		},
	}

	for _, t := range tests {
		log.Printf("executing test %q", t.name)
		log.Printf("description: %s", t.description)
		flog.Println()
		flog.Print(t.name)
		flog.Println()

		if err := t.execute(client); err != nil {
			return err
		}
	}
	log.Print("all tests passed")
	log.Print("comments:")
	for _, t := range tests {
		for _, c := range t.comments {
			log.Printf("test %q: %s", t.name, c)
		}
	}

	return nil
}

func handleUserExit(cmd *exec.Cmd) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Kill, os.Interrupt)
	sig := <-sigCh
	log.Printf("user exit: %s", sig)
	cmd.Process.Kill()
	os.Exit(0)
}

func monitorExit(cmd *exec.Cmd, exitCh chan int) {
	err := cmd.Wait()
	log.Printf("plugin exited")
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		log.Fatalf("plugin exited with error: %s", err)
	}

	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		exitCh <- status.ExitStatus()
	}
	exitCh <- 0
}

func monitorStdout(r io.Reader, portCh chan int) {
	scanner := bufio.NewScanner(r)
	scanner.Scan()
	port, err := strconv.Atoi(scanner.Text())
	if err != nil {
		log.Fatalf("bad port number %q: %s", scanner.Text(), err)
	}
	log.Printf("got port: %d", port)
	portCh <- port

	pluginLog := golog.New(os.Stdout, color.YellowString("PLUGIN|"), golog.Ltime|golog.Lmicroseconds)

	// write the rest of the plugin's output to stdout
	for scanner.Scan() {
		pluginLog.Print(scanner.Text())
	}
}

var schemaPeople = plugin.Schema{
	Name: "people",
	Properties: []*plugin.Property{
		{
			Name: "id",
			Type: "integer",
		}, {
			Name: "first_name",
			Type: "string",
		}, {
			Name: "last_name",
			Type: "string",
		}, {
			Name: "email",
			Type: "string",
		}, {
			Name: "gender",
			Type: "string",
		}, {
			Name: "ip_address",
			Type: "string",
		},
	},
}
var schemaAnimals = plugin.Schema{
	Name: "animals",
	Properties: []*plugin.Property{
		{
			Name: "id",
			Type: "integer",
		}, {
			Name: "name",
			Type: "string",
		}, {
			Name: "extinct",
			Type: "boolean",
		}, {
			Name: "last spotted",
			Type: "datetime",
		},
	},
}
var schemaLogs = plugin.Schema{
	Name: "logs",
	Properties: []*plugin.Property{
		{
			Name: "timestamp",
			Type: "datetime",
		}, {
			Name: "event",
			Type: "string",
		}, {
			Name: "magnitude",
			Type: "number",
		},
	},
}

type testCase struct {
	name            string
	description     string
	glob            string
	expectedSchemas []plugin.Schema
	publishSchema   plugin.Schema
	expectedRecords expectedRecords
	expectedCount   int
	comments        []string
}

type expectedRecord struct {
	matchIndex int
	matchValue interface{}
	invalid    bool
	match      *plugin.PublishRecord
}

type expectedRecords []*expectedRecord

func (r expectedRecords) evaluate(record *plugin.PublishRecord) {
	for i := range r {
		e := r[i]
		var data []interface{}
		json.Unmarshal([]byte(record.Data), &data)
		if data[e.matchIndex] == e.matchValue {
			e.match = record
		}
	}
}

func (t *testCase) execute(client plugin.PluginClient) error {
	l := func(f string, args ...interface{}) {
		log.Printf("test "+t.name+": "+f, args...)
	}
	l("executing discover...")

	settings := &plugin.Settings{
		FileGlob: t.glob,
	}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	discover, err := client.Discover(ctx, &plugin.DiscoverRequest{
		Settings: settings,
	})
	if err != nil {
		return errors.WithMessage(err, "discovery failed")
	}
	l("discover completed: %s", discover)
	l("scoring discover...")

	j, _ := json.MarshalIndent(discover, "", "  ")
	flog.Println("discover response:")
	flog.Println(string(j))

	for _, want := range t.expectedSchemas {
		namesMatch, typesMatch := checkSchemaIn(want, discover.Schemas)
		if !namesMatch {
			return errors.Errorf("no schema matclng %q was discovered (want: %s, got: %s)", want.Name, want, discover.Schemas)
		}
		if typesMatch {
			t.comments = append(t.comments, color.GreenString("inferred types on schema %s: ", want.Name) + want.String())
		} else {
			t.comments = append(t.comments, color.RedString("did not infer types on schema %s: ", want.Name) + want.String())
		}
	}
	l("discover looks correct")
	l("executing publish...")
	flog.Println()

	targetSchema := findSchemaIn(t.publishSchema, discover.Schemas)

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := client.Publish(ctx, &plugin.PublishRequest{
		Settings: settings,
		Schema:   targetSchema,
	})
	if err != nil {
		return errors.Wrap(err, "publish failed")
	}

	var count = 0
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Errorf("publish error on record %d: %s", count, err)
		}
		count++
		j, _ = json.Marshal(record)
		flog.Println(string(j))
		t.expectedRecords.evaluate(record)
	}
	l("publish completed, analyzing data...")

	if count != t.expectedCount {
		return errors.Errorf("publish did not return the right number of records (wanted %d, got %d)", t.expectedCount, count)
	}

	l("publish has correct count, %d", count)

	for _, e := range t.expectedRecords {
		if e.match == nil {
			return errors.Errorf("record not published (should have had record with %v at data index %d)", e.matchValue, e.matchIndex)
		}
		if e.invalid {
			if e.match.Invalid {
				t.comments = append(t.comments, color.GreenString("detected invalid record: ")+e.match.String())
			} else {
				t.comments = append(t.comments, color.RedString("record should have been marked invalid: ")+e.match.String())
			}
		}
	}
	l("published data looks correct")

	return nil
}

func checkSchemaIn(want plugin.Schema, in []*plugin.Schema) (namesMatch bool, typesMatch bool) {

	for _, found := range in {
		if namesMatch, typesMatch = checkSchema(want, found); namesMatch {
			return
		}
	}

	return false, false
}

func findSchemaIn(want plugin.Schema, in []*plugin.Schema) (found *plugin.Schema) {

	for _, found = range in {
		if namesMatch, _ := checkSchema(want, found); namesMatch {
			return
		}
	}

	return nil
}

func checkSchema(want plugin.Schema, have *plugin.Schema) (namesMatch bool, typesMatch bool) {

	if len(want.Properties) != len(have.Properties) {
		return false, false
	}
	namesMatch = true
	typesMatch = true

	for i, pw := range want.Properties {
		ph := have.Properties[i]
		namesMatch = namesMatch && pw.Name == ph.Name
		typesMatch = typesMatch && pw.Type == ph.Type
	}
	return
}
