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
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
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

	stdoutReader, stdoutWriter := io.Pipe()
	cmd := exec.Command(os.Args[1], os.Args[2:]...)

	cmd.Stderr = os.Stdout
	cmd.Stdout = stdoutWriter

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
		log.Fatalf("did not get a port from the plugin within timeout of %s", pluginStartupTimeout)
	case exitCode := <-exitCh:
		if exitCode != 0 {
			log.Fatalf("plugin exited with non-zero code %d", exitCode)
		}
		os.Exit(exitCode)
	case port := <-portCh:
		err := runTests(port)
		if err != nil {
			os.Exit(1)
		}
	}
}

func runTests(port int) error {
	addr := fmt.Sprintf("localhost:%d", port)
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithReadBufferSize(500))
	if err != nil {
		return errors.WithMessage(err, "connection failed")
	}

	client := plugin.NewPluginClient(conn)

	pwd, _ := os.Getwd()
	tests := []test{
		&standardTestCase{
			n:               "animals",
			d:               `This test exercises schema type discovery, because "animals.csv" has multiple data types`,
			glob:            filepath.Join(pwd, "./data/animals.csv"),
			expectedCount:   100,
			publishSchema:   schemaAnimals,
			expectedSchemas: []plugin.Schema{schemaAnimals},
			recordChecks: expectedRecords{
				requiredRecordCheck(1, "Vulpes chama"),
				invalidRecordCheck(1, "Macropus fuliginosus", " because blue is not a valid boolean"),
				parsingRecordCheck(0, float64(52), 0, float64(52), " because id column should be parsed as number"),
				parsingRecordCheck(0, float64(83), 3, timeFromRFC3339String("1796-07-23T00:00:00.000Z"), ` because "last spotted" column should be parsed as date`),
			},
		},
		&standardTestCase{
			n:               "logs",
			d:               "This test checks that schemas are based on headers in files, and that the plugin can handle complex data.",
			glob:            filepath.Join(pwd, "./data/*.csv"),
			expectedCount:   300,
			publishSchema:   schemaLogs,
			expectedSchemas: []plugin.Schema{schemaAnimals, schemaLogs, schemaPeople},
			recordChecks: expectedRecords{
				requiredRecordCheck(1, "社會科學院語學研究所"),
				requiredRecordCheck(1, "Ω≈ç√∫˜µ≤≥÷"),
				parsingRecordCheck(1, "normal", 2, float64(27.78092), " because magnitude should be parsed as number"),
			},
		},
		&standardTestCase{
			n:               "people",
			d:               "This test checks that the plugin can publishes large amounts of data quickly.",
			glob:            filepath.Join(pwd, "./data/people.*.csv"),
			expectedCount:   3000,
			publishSchema:   schemaPeople,
			expectedSchemas: []plugin.Schema{schemaLogs, schemaPeople},
			recordChecks: expectedRecords{
				requiredRecordCheck(3, "lroylr4@indiatimes.com"),
				requiredRecordCheck(3, "mbranstoncs@mit.edu"),
				requiredRecordCheck(3, "bmageei@linkedin.com"),
			},
		},
		&standardTestCase{
			n:               "garbage",
			d:               `This test checks if any types have been inferred from a very unclean data set.`,
			glob:            filepath.Join(pwd, "./data/garbage.csv"),
			expectedCount:   10,
			publishSchema:   schemaGarbage,
			expectedSchemas: []plugin.Schema{schemaGarbage},
			recordChecks: expectedRecords{
				requiredRecordCheck(0, "a"),
				parsingRecordCheck(0, "a", 1, "1", " because 'interleaved' column should be inferred to be a string"),
				parsingRecordCheck(0, "b", 2, nil, " because 'count' column should be inferred to be a number, and 'seventeen' is not a valid number"),
				parsingRecordCheck(0, "d", 3, true, " because 'is' column should be inferred to be a boolean, and 'True' is reasonably parsable as a boolean"),
				parsingRecordCheck(0, "g", 4, "12", " because 'math' column should be inferred to be a string"),
				parsingRecordCheck(0, "i", 6, timeFromRFC3339String("1970-01-06T16:57:07.445Z"), " because 'epoch' column could be inferred to be a date, maybe"),
			},
		},
	}

	var results []*testResult
	total := len(tests)
	failCount := 0

	for i, t := range tests {
		log.Printf("%d/%d: executing test %q", i+1, total, t.name())
		log.Printf("description: %s", t.description())
		flog.Println(strings.Repeat("-", 50))
		flog.Print(t.name())
		flog.Println(strings.Repeat("-", 50))

		result := t.execute(client)
		result.test = t
		if result.err != nil {
			failCount++
			log.Print(color.RedString("test %s failed: %s"), t.name(), result.err)
		} else {
			log.Print(color.GreenString("test %s passed"), t.name())
		}
		results = append(results, result)
	}

	color.Blue("RESULTS")
	good := color.New(color.Bold, color.FgGreen)
	bad := color.New(color.Bold, color.FgRed)
	for _, result := range results {
		fmt.Printf("%s: ", result.test.name())
		if result.err != nil {
			bad.Printf("failed: %s\n", result.err)
		} else {
			good.Println("passed")
		}
		color.New(color.Faint, color.FgWhite).Printf("  %s\n", result.test.description())
		for _, c := range result.comments {
			fmt.Println("  " + c)
		}
	}

	if failCount == 0 {
		good.Println("PASSED")
		return nil
	} else {
		bad.Printf("%d TESTS FAILED\n", failCount)
		color.Yellow("see .log file for all data processed")

		return errors.New("failed")
	}
}

func timeFromRFC3339String(t string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(fmt.Sprintf("Write the date correctly, %q isn't valid: %s", t, err))
	}
	return parsed
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
var schemaGarbage = plugin.Schema{
	Name: "garbage",
	Properties: []*plugin.Property{
		{
			Name: "key",
			Type: "string",
		}, {
			Name: "interleaved",
			Type: "string",
		}, {
			Name: "count",
			Type: "integer",
		}, {
			Name: "is",
			Type: "boolean",
		}, {
			Name: "math",
			Type: "string",
		}, {
			Name: "result",
			Type: "number",
		}, {
			Name: "epoch",
			Type: "date",
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

type test interface {
	execute(client plugin.PluginClient) (*testResult)
	name() string
	description() string
}

type standardTestCase struct {
	n               string
	d               string
	glob            string
	expectedSchemas []plugin.Schema
	publishSchema   plugin.Schema
	recordChecks    expectedRecords
	expectedCount   int
	comments        []string
}

func (t *standardTestCase) name() string {
	return t.n
}

func (t *standardTestCase) description() string {
	return t.d
}

type testResult struct {
	test     test
	err      error
	comments []string
}

func (t *testResult) withErr(err error) *testResult {
	t.err = err
	return t
}

func (t *testResult) comment(format string, args ...interface{}) *testResult {
	if len(format) > 0 {
		t.comments = append(t.comments, fmt.Sprintf(format, args...))
	}
	return t
}

func (t *testResult) log(format string, args ...interface{}) {
	log.Printf(color.CyanString(t.test.name()+": ")+format, args...)
}

type recordCheck struct {
	matchIndex      int
	matchValue      interface{}
	isParseCheck    bool
	checkIndex      int
	checkValue      interface{}
	isBonus         bool
	shouldBeInvalid bool
	match           *plugin.PublishRecord
	reason          string
	parseErr        error
}

func requiredRecordCheck(index int, value interface{}) *recordCheck {
	return &recordCheck{
		matchIndex: index,
		matchValue: value,
	}
}

func invalidRecordCheck(index int, value interface{}, reason string) *recordCheck {
	return &recordCheck{
		matchIndex:      index,
		matchValue:      value,
		isBonus:         true,
		shouldBeInvalid: true,
		reason:          reason,
	}
}

func parsingRecordCheck(keyIndex int, keyValue interface{}, checkIndex int, checkValue interface{}, reason string) *recordCheck {
	return &recordCheck{
		matchIndex:   keyIndex,
		matchValue:   keyValue,
		checkIndex:   checkIndex,
		checkValue:   checkValue,
		isParseCheck: true,
		isBonus:      true,
		reason:       reason,
	}
}

func (r *recordCheck) evaluate(record *plugin.PublishRecord, data []interface{}) {
	if r.match != nil {
		return
	}
	if r.matchIndex >= len(record.Data) {
		return
	}

	actual := data[r.matchIndex]
	if actual == r.matchValue {
		r.match = record
	}

	r.parseErr = r.evaluateParsing(record, data)
}

func (r *recordCheck) evaluateParsing(record *plugin.PublishRecord, data []interface{}) error {
	if !r.isParseCheck {
		return nil
	}
	if r.checkIndex >= len(record.Data) {
		return errors.New("record too narrow")
	}

	actual := data[r.checkIndex]
	if actual == nil {
		if r.checkValue == nil {
			return nil
		} else {
			return errors.Errorf("expected value at %s to be nil/null", r.checkIndex)
		}
	}

	switch expectedValue := r.checkValue.(type) {
	case time.Time:
		actualString, _ := actual.(string)
		actualTime, err := time.Parse(time.RFC3339Nano, actualString)
		if err != nil {
			return errors.Errorf("expected value at %d to be valid date: %s", r.checkIndex, err)
		}
		if actualTime != expectedValue {
			return errors.Errorf("expected value at %d to equal %v but it was %v", r.checkIndex, expectedValue, actualTime)
		}
	case float64:
		actualFloat, ok := actual.(float64)
		if !ok  {
			return errors.Errorf("expected value at %d (%v) to be a number, but it was a %T", r.checkIndex, actual, actual)
		}
		if diff := math.Abs(actualFloat - expectedValue); diff > 0.00001 {
			return errors.Errorf("expected value at %d to equal %v but it was %v", r.checkIndex, expectedValue, actualFloat)
		}
	default:
		if actual != r.matchValue {
			return errors.Errorf("expected value at %d to equal %v (%T) but it was %v (%T)", r.checkIndex, expectedValue, expectedValue, actual, actual)
		}
	}

	return nil
}

func (r *recordCheck) result() (ok bool, msg string) {
	if r.match == nil {
		return false, color.RedString("expected to see a record with value %v at data index %d%s", r.matchValue, r.matchIndex, r.reason)
	} else {
		if r.shouldBeInvalid {
			if r.match.Invalid {
				return true, color.GreenString("detected invalid record { %s }", r.match)
			} else {
				return false, color.RedString("record should have been marked invalid%s: { %s }", r.reason, r.match)
			}
		} else if r.isParseCheck {
			if r.parseErr == nil {
				return true, color.GreenString("correctly parsed record { %s }", r.match)
			} else {
				return true, color.RedString("parsing failed on record { %s }: %s", r.match, r.parseErr)
			}
		}
	}
	return true, ""
}

type expectedRecords []*recordCheck

func (r expectedRecords) evaluate(record *plugin.PublishRecord) {
	var data []interface{}
	json.Unmarshal([]byte(record.Data), &data)
	for _, expected := range r {
		expected.evaluate(record, data)
	}
}

func (t *standardTestCase) execute(client plugin.PluginClient) *testResult {
	result := &testResult{
		test: t,
	}
	result.log("executing discover...")

	settings := &plugin.Settings{
		FileGlob: t.glob,
	}
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	discover, err := client.Discover(ctx, &plugin.DiscoverRequest{
		Settings: settings,
	})
	if err != nil {
		return result.withErr(errors.WithMessage(err, "discovery failed"))
	}
	result.log("discover completed: %s", discover)
	result.log("scoring discover...")

	j, _ := json.MarshalIndent(discover, "", "  ")
	flog.Println("discover response:")
	flog.Println(string(j))

	for _, want := range t.expectedSchemas {
		namesMatch, typesMatch, got := checkSchemaIn(want, discover.Schemas)
		if !namesMatch {
			return result.withErr(errors.Errorf("no schema matclng %q was discovered (want: %s, got: %s)", want.Name, want, discover.Schemas))
		}
		if typesMatch {
			result.comment(color.GreenString("inferred types on schema %s: ", want.Name) + want.String())
		} else {

			w := new(strings.Builder)
			for i := range got.Properties {
				wp := want.Properties[i]
				gp := got.Properties[i]
				if wp.Type != gp.Type {
					fmt.Fprintf(w, "%s: wanted %s, got %s; ", wp.Name, color.GreenString(wp.Type), color.RedString(gp.Type))
				}
			}

			result.comment(color.RedString("did not infer types on schema %s: ", want.Name) + w.String())
		}
	}
	result.log("discover looks correct")
	result.log("executing publish...")
	flog.Println()

	targetSchema := findSchemaIn(t.publishSchema, discover.Schemas)

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	stream, err := client.Publish(ctx, &plugin.PublishRequest{
		Settings: settings,
		Schema:   targetSchema,
	})
	if err != nil {
		return result.withErr(errors.Wrap(err, "publish failed"))
	}

	var count = 0
	for {
		record, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result.withErr(errors.Errorf("publish error on record %d: %s", count, err))
		}
		count++
		j, _ = json.MarshalIndent(record, "", "  ")
		flog.Println(string(j))
		t.recordChecks.evaluate(record)
	}
	result.log("publish completed, analyzing data...")

	if count != t.expectedCount {
		return result.withErr(errors.Errorf("publish did not return the right number of records (wanted %d, got %d)", t.expectedCount, count))
	}

	result.log("publish has correct count, %d", count)

	for _, e := range t.recordChecks {
		ok, msg := e.result()
		if ok {
			result.comment(msg)
		} else {
			if e.isBonus {
				result.comment(msg)
			} else {
				return result.withErr(errors.Errorf("record check failed: %s", msg))
			}
		}
	}
	result.log("published data looks correct")

	return result
}

var mismatch = color.New(color.CrossedOut, color.FgRed)

func checkSchemaIn(want plugin.Schema, in []*plugin.Schema) (namesMatch bool, typesMatch bool, found *plugin.Schema) {

	for _, found := range in {
		if namesMatch, typesMatch = checkSchema(want, found); namesMatch {
			return namesMatch, typesMatch, found
		}
	}

	return false, false, nil
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
