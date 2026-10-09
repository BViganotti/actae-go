package main

import (
	"context"
	"fmt"
	"log"

	actae "github.com/BViganotti/actae-go"
)

func main() {
	runtime, err := actae.NewRuntimeFromEnv(actae.ClientOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	err = runtime.Run(context.Background(), actae.RunOptions{
		ID: "order-42", Framework: "example",
	}, func(ctx context.Context, run actae.Scope) error {
		result, effectErr := actae.Do(
			ctx,
			run,
			"order:42:receipt",
			"create_receipt",
			map[string]any{"order_id": "42", "amount": 2500},
			func(context.Context) (map[string]any, error) {
				return map[string]any{"receipt_id": "receipt-42", "amount": 2500}, nil
			},
		)
		if effectErr == nil {
			fmt.Println(result)
		}
		return effectErr
	})
	if err != nil {
		log.Fatal(err)
	}
}
