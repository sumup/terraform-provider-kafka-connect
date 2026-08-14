package connect

import (
	"context"
	"errors"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func kafkaConnectBackfillResource() *schema.Resource {
	return &schema.Resource{
		Create: backfillExecute, // Called when Terraform processes this block
		Read:   backfillRead,    // Empties state immediately after creation
		Delete: backfillDelete,
		Schema: map[string]*schema.Schema{
			"backfill_id": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The UUID of the backfill task to execute.",
			},
		},
	}
}

func backfillExecute(d *schema.ResourceData, meta interface{}) error {
	metaStruct := meta.(*ProviderMeta)
	client := metaStruct.TitanicClient
	backfillID := d.Get("backfill_id").(string)

	log.Printf("[INFO] Extracting base URL from Kafka Connect configuration")

	err := client.ExecuteBackfill(context.Background(), backfillID)
	if err != nil {
		return errors.New("could not execute backfill: " + err.Error() + " with id: " + backfillID)
	}

	d.SetId(backfillID)
	return nil
}

func backfillRead(d *schema.ResourceData, meta interface{}) error {
	// Empty out the resource identifier immediately.
	// This leaves `.tfstate` clean and forces a re-run on subsequent executions.
	d.SetId("")
	return nil
}

func backfillDelete(d *schema.ResourceData, meta interface{}) error {
	// Operational trigger only. No remote infrastructure destruction required.
	return nil
}
