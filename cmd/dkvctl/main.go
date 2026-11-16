// Command dkvctl is the dkv command-line client.
//
//	dkvctl [flags] put <key> <value>
//	dkvctl [flags] get <key>
//	dkvctl [flags] delete <key>
//
// Flags go before the command. Exit status follows grep: 0 on success,
// 1 when get finds no value, 2 on any error.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naveenk2608/distributed-database/pkg/client"
)

const (
	exitOK       = 0
	exitNotFound = 1
	exitError    = 2
)

// addrEnv deliberately avoids the DKV_ prefix, which belongs to the server's
// configuration and is checked strictly there.
const addrEnv = "DKVCTL_ADDR"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// output writes results to stdout and diagnostics to stderr. A failed
// stdout write (a full disk, a closed pipe) is remembered so the command can
// exit with an error instead of reporting success. Diagnostics are best
// effort: there is nowhere left to report a failed stderr write.
type output struct {
	stdout, stderr io.Writer
	err            error
}

func (o *output) result(format string, args ...any) {
	if _, err := fmt.Fprintf(o.stdout, format, args...); err != nil && o.err == nil {
		o.err = err
	}
}

func (o *output) resultBytes(b []byte) {
	if _, err := o.stdout.Write(b); err != nil && o.err == nil {
		o.err = err
	}
}

func (o *output) errorf(format string, args ...any) {
	_, _ = fmt.Fprintf(o.stderr, "dkvctl: "+format+"\n", args...)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	out := &output{stdout: stdout, stderr: stderr}
	code := runCommand(ctx, args, getenv, out)
	if out.err != nil {
		out.errorf("writing output: %v", out.err)
		return exitError
	}
	return code
}

func runCommand(ctx context.Context, args []string, getenv func(string) string, out *output) int {
	stderr := out.stderr
	fs := flag.NewFlagSet("dkvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", getenv(addrEnv), "server address host:port (env "+addrEnv+")")
	timeout := fs.Duration("timeout", 5*time.Second, "deadline for the request")
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `Usage: dkvctl [flags] <command> [args]

Commands:
  put <key> <value>   store value under key; prints the new version
  get <key>           print the value of key (exit status 1 if not found)
  delete <key>        delete key; prints the version of the delete

Flags:
`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitError
	}

	usage := func(msg string) int {
		out.errorf("%s\n", msg)
		fs.Usage()
		return exitError
	}
	cmd := fs.Args()
	if len(cmd) == 0 {
		return usage("missing command")
	}
	wantArgs := map[string]int{"put": 2, "get": 1, "delete": 1}
	n, ok := wantArgs[cmd[0]]
	if !ok {
		return usage(fmt.Sprintf("unknown command %q", cmd[0]))
	}
	if len(cmd)-1 != n {
		return usage(fmt.Sprintf("%s takes %d argument(s), got %d", cmd[0], n, len(cmd)-1))
	}
	if *addr == "" {
		return usage("no server address: pass --addr or set " + addrEnv)
	}

	c, err := client.New(*addr)
	if err != nil {
		out.errorf("%v", err)
		return exitError
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	key := []byte(cmd[1])
	switch cmd[0] {
	case "put":
		ver, err := c.Put(ctx, key, []byte(cmd[2]))
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		out.result("OK (version %d)\n", ver)
	case "get":
		val, _, err := c.Get(ctx, key)
		if errors.Is(err, client.ErrNotFound) {
			out.errorf("key %q not found", cmd[1])
			return exitNotFound
		}
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		out.resultBytes(append(val, '\n'))
	case "delete":
		found, ver, err := c.Delete(ctx, key)
		if err != nil {
			out.errorf("%v", err)
			return exitError
		}
		if found {
			out.result("deleted (version %d)\n", ver)
		} else {
			out.result("key did not exist (version %d)\n", ver)
		}
	}
	return exitOK
}
