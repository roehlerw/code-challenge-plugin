# Solution: Node Plugin

Solution to the problem written in node.

### Running Solution

To run the solution it requires [node](https://nodejs.org/en/) version 8.12.0+ to be installed.

To install the required node modules, run this command inside the /plugin/node directory.
```bash
npm install
```

To run the host and test the plugin run this command in the root directory of the project.
```bash
go run host.go node "$PWD\plugin\node\plugin.js"
```