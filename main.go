package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	// "github.com/go-kit/kit/log/level"
	"github.com/go-redis/redis/v7"
	"github.com/kolide/kit/actor"
	"github.com/oklog/run"
	"github.com/pkg/errors"
)

var streamname = "testing:stream"
var groupname = "testing:stream:consumergroup0"

type worker struct {
	logger log.Logger
	client *redis.Client
}

func main() {
	client, err := newRedisClient()
	if err != nil {
		fmt.Errorf("creating client: %v", err)
		os.Exit(1)
	}

	runcontext, runcancel := context.WithCancel(context.TODO())
	defer runcancel()

	err = setupConsumerGroup(&client)
	if err != nil {
		fmt.Errorf("unable to create consumer group: %v", err)
	}

	lev := level.AllowInfo()
	base := log.NewJSONLogger(log.NewSyncWriter(os.Stdout))
	base = level.NewInjector(base, level.InfoValue())
	base = log.With(base, "caller", log.Caller(6))
	logger := level.NewFilter(base, lev)
	var g run.Group

	switch command := getSubCommand(); command {
	case "produce":
		p := worker{logger: logger, client: &client}
		producer := &actor.Actor{
			Execute: func() error {
				return p.produceData(runcontext)
			},
			Interrupt: func(err error) {
				runcontext.Done()
			},
		}
		g.Add(producer.Execute, producer.Interrupt)

	case "consume":
		c := worker{logger: logger, client: &client}
		consumer1 := &actor.Actor{
			Execute: func() error {
				return errors.Wrap(c.consumeData(runcontext, "alice"), "alice consuming")
			},
			Interrupt: func(err error) {
				runcontext.Done()
			},
		}
		consumer2 := &actor.Actor{
			Execute: func() error {
				return errors.Wrap(c.consumeData(runcontext, "bob"), "bob consuming")
			},
			Interrupt: func(err error) {
				runcontext.Done()
			},
		}
		g.Add(consumer1.Execute, consumer1.Interrupt)
		g.Add(consumer2.Execute, consumer2.Interrupt)
	}

	// // add interrupt handler
	{
		sig := make(chan os.Signal, 1)
		g.Add(func() error {
			signal.Notify(sig, os.Interrupt)
			<-sig
			fmt.Println("beginning shutdown")
			return nil
		}, func(err error) {
			fmt.Printf("process interrupted, err: %v\n", err)
			close(sig)
		},
		)
	}
	g.Run()
}

func newRedisClient() (client redis.Client, err error) {
	client = *redis.NewClient(&redis.Options{
		Addr:     "localhost:6397",
		Password: "", // no password
		DB:       0,  // use default db
	})

	_, err = client.Ping().Result()
	if err != nil {
		fmt.Errorf("verifying redis ping: %v", err)
	}
	return client, err
}

func (w *worker) produceData(ctx context.Context) error {
	words := [3]string{"apple", "orange", "pear"}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	fn := func() error {
		args := &redis.XAddArgs{
			Stream: streamname,
			Values: map[string]interface{}{
				"ts":    time.Now().Unix(),
				"fruit": words[rand.Intn(len(words))],
			},
		}
		cmd := w.client.XAdd(args)
		w.client.Process(cmd)
		if err := cmd.Err(); err != nil {
			return errors.Wrap(err, "xadd-ing to stream")
		}
		result, err := cmd.Result()
		if err != nil {
			return errors.Wrap(err, "getting xadd command results")
		}

		level.Info(w.logger).Log(
			"message", "added to stream",
			"result", result,
		)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := fn()
			if err != nil {
				return err
			}
		}
	}
}

func (w *worker) consumeData(ctx context.Context, consumerName string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	fn := func() error {
		args := &redis.XReadGroupArgs{
			Group:    groupname,
			Consumer: consumerName,
			Streams:  []string{streamname, ">"},
			Count:    1,
			NoAck:    true,
		}
		cmd := w.client.XReadGroup(args)
		w.client.Process(cmd)
		if err := cmd.Err(); err != nil {
			return errors.Wrap(err, "reading via xreadgroup")
		}
		results, err := cmd.Result()
		if err != nil {
			return errors.Wrap(err, "getting results of xreadgroup")
		}
		level.Info(w.logger).Log(
			"consumer", consumerName,
			"message", results[0].Messages[0].Values, // we only ask for 1 message, remember?
		)

		ackCmd := w.client.XAck(streamname, groupname, results[0].Messages[0].ID)
		w.client.Process(ackCmd)
		if err := ackCmd.Err(); err != nil {
			return errors.Wrap(err, "ack-ing messages")
		}

		level.Debug(w.logger).Log(
			"consumer", consumerName,
			"action", "XACKed message",
		)
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := fn()
			if err != nil {
				return err
			}
		}
	}
}

func setupConsumerGroup(client *redis.Client) error {
	cmd := client.XGroupCreate(streamname, groupname, "$")
	client.Process(cmd)
	if err := cmd.Err(); err != nil {
		return errors.Wrap(err, "creating consumer group")
	}

	return nil
}

func getSubCommand() string {
	if len(os.Args) < 2 {
		fmt.Println("no subcommand provided: need setup, produce or consume")
		os.Exit(1)
	}

	subCommands := []string{
		"setup",
		"produce",
		"consume",
	}

	for _, sc := range subCommands {
		if sc == os.Args[1] {
			return sc
		}
	}

	fmt.Sprintf("invalid subcommand '%s', not one of %v", os.Args[1], subCommands)
	os.Exit(1)
	return ""
}
