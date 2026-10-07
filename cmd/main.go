// Copyright 2026 Digital Clever Solution LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package main is the entry point for the iguazu-coordinator sidecar binary.
// It connects to the coordinator beacon node over gRPC and to the gwat
// execution layer over JSON-RPC, then drives the dag-finalization loop.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LFDT-Iguazu/iguazu-coordinator/finalization"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var log = logrus.WithField("prefix", "main")

var (
	flagCoordinatorGRPC = &cli.StringFlag{
		Name:    "coordinator-grpc",
		Usage:   "coordinator gRPC endpoint (host:port or unix:///path)",
		EnvVars: []string{"WF_COORDINATOR_GRPC"},
		Value:   "localhost:4000",
	}
	flagGwatHTTP = &cli.StringFlag{
		Name:    "gwat-http",
		Usage:   "gwat JSON-RPC HTTP endpoint",
		EnvVars: []string{"WF_GWAT_HTTP"},
		Value:   "http://localhost:8545",
	}
	flagLogLevel = &cli.StringFlag{
		Name:    "log-level",
		Usage:   "log level (trace, debug, info, warn, error)",
		EnvVars: []string{"WF_LOG_LEVEL"},
		Value:   "info",
	}
)

func main() {
	app := &cli.App{
		Name:  "iguazu-coordinator",
		Usage: "Iguazu sidecar: dag-finalization loop (Apache 2.0)",
		Flags: []cli.Flag{
			flagCoordinatorGRPC,
			flagGwatHTTP,
			flagLogLevel,
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cliCtx *cli.Context) error {
	level, err := logrus.ParseLevel(cliCtx.String(flagLogLevel.Name))
	if err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}
	logrus.SetLevel(level)

	coordEndpoint := cliCtx.String(flagCoordinatorGRPC.Name)
	gwatEndpoint := cliCtx.String(flagGwatHTTP.Name)

	log.WithFields(logrus.Fields{
		"coordinator": coordEndpoint,
		"gwat":        gwatEndpoint,
	}).Info("iguazu-coordinator starting")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.WithField("signal", sig).Info("Caught signal — shutting down")
		cancel()
	}()

	coordConn, err := grpc.Dial(coordEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial coordinator gRPC %s: %w", coordEndpoint, err)
	}
	defer coordConn.Close()

	var gwatClient *finalization.GwatClient
	for {
		gwatClient, err = finalization.NewGwatClient(gwatEndpoint)
		if err == nil {
			break
		}
		log.WithError(err).Warn("gwat not ready, retrying in 5s")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
	defer gwatClient.Close()

	svc := finalization.NewService(coordConn, gwatClient)
	svc.Run(ctx)

	log.Info("iguazu-coordinator stopped")
	return nil
}
