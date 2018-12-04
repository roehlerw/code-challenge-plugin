# Code Challenge: Toy Plugin

In this code challenge you will construct a simplified version of the kind of plugin that Naveego uses to discover and collect data. The plugin will be responsible for discovering and collecting data from CSV files.

### Scenario

Our plugin framework uses gRPC for communication. In reality we use the Hashicorp [go-plugin](https://github.com/hashicorp/go-plugin) framework, but
for this challenge we will just use plain gRPC.

Using a language with [gRPC support](https://grpc.io/docs/), you must produce a CLI app which will satisfy the plugin protocol defined below. Your app will act as the gRPC server.

This directory contains a fake plugin host which will run your plugin and call it as a gRPC client. The
host is a CLI app which you can run using Go (Go 1.11 required, see here: https://golang.org/doc/install)

```bash
go run host.go {command to start your program}
```

The host will run your app using the command you pass to it, and will exercise it by making calls over gRPC.

For example, if your plugin implementation were a binary named `impl` in the same directory as host.go, you would run 
```bash
go run host.go ./impl
```

> The command must be run in the root of this repository, as the host expects to find the ./data directory in its $PWD.

If invoking your implementation is complex, consider creating a shell script which handles the invocation and passing that script to host.go.

If your implementation has extensive environmental dependencies (i.e., Python versions or .NET Core),
please use the Dockerfile to build a run context. Just install your dependencies and build your implementation
after the ENTRYPOINT directive and set the CMD to invoke your implementation. Then test your command by

```bash
docker run -it --rm $(docker build -q .)
```


### Plugin Protocol

The plugin must claim a port and start a gRPC server on that port. After claiming the port,
the plugin must write the port number to stdout, followed by a carriage return. The plugin
must not write anything else to stdout before writing the port number. After it writes the port 
number, it can emit logging messages to stdout or stderr.

The plugin should allow insecure connections.

The plugin must stop streaming data and exit with code 0 if it receives an
SIGINT or SIGKILL.

The gRPC server the plugin starts must fulfil the contract defined in [./plugin.proto](./plugin.proto). The host will first call the Discover method and will expect to get back a listing of schemas. Then it will
call the Publish method for each schema and will expect to be streamed
the data from the files for that schema.

For details about the contract, see the comments in [./plugin.proto](./plugin.proto).


### Workflow

If you want to attempt this challenge, fork this repo, push your solution, then send a pull request for us to review. If your solution has dependencies, please explain how to install them in your pull request, or use the Dockerfile to provide a self-contained context.
 
>Looking at other people's pull requests before you've completed your own solution is rude. 

Your solution will be evaluated on several dimensions:

1. Accuracy: Your code should pass the tests as run by the host. Note that it's ok if some of the results show in red, as long as the final result is PASSED.
2. Clarity: Your code should be easy to understand and read. It should also be easy to extend in the future,
as plugin contracts evolve.
3. Production readiness: Your code should provide sufficient logging information to support debugging production issues. 
It should also provide good, helpful error messages when things go wrong, and it should try very hard not to silently crash.
4. Sophistication: The contract includes fields where you can place the inferred type of properties, and fields for indicating that 
a record does not conform to the inferred schema. Consider population of these fields extra credit - it may be very 
 difficult (or impossible) to accurately infer the correct values, but we will be impressed if you can. In particular,
 the file [./data/garbage.csv](./data/garbage.csv) will be difficult to make inferences about.
 
We're hoping this challenge will help you demonstrate that you are able to learn new frameworks quickly, build out a project from scratch based on a rough set of specs, and write production-quality code. If we schedule an interview, we'll use your solution to drive a conversation about your code, as well as potential issues that you might have found in the contract itself. Think about how you would improve and evolve this contract to add functionality in the future. 

If you find that the test harness is doing something wrong, or you're completely stuck on how to solve something, file an issue against this repo. If you can send a pull request to improve the test harness that would be even better.

(Please don't judge the quality of the code in host.go too harshly; it was written in a hurry with a lot of ⸢help⸣ from a four year old. We'll try to clean it up in the future.)

Sample message
