package connect

import (
	"context"
	"fmt"
	"log"

	"github.com/Mongey/terraform-provider-kafka-connect/titanic"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// statusPending is the Titanic status of a backfill that has not been triggered
// yet. It is the only status this resource executes: every other status means the
// backfill has already run, so executing it again would duplicate a snapshot.
const statusPending = "pending"

// statusNotFound is recorded when Titanic no longer lists a backfill that is
// present in Terraform state. Titanic omits deleted backfills from the listing,
// so this most likely means the backfill was deleted after it was executed.
const statusNotFound = "not_found"

func kafkaConnectBackfillResource() *schema.Resource {
	return &schema.Resource{
		Create: backfillExecute, // Called when Terraform processes this block
		Read:   backfillRead,    // Refreshes status from Titanic for the connector
		Delete: backfillDelete,
		Importer: &schema.ResourceImporter{
			State: backfillImport,
		},
		Schema: map[string]*schema.Schema{
			"connector_name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The Kafka Connect connector name whose backfills should be managed.",
			},
			"backfill_id": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The UUID of the backfill task to execute.",
			},
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The status reported by Titanic for this backfill at the last refresh.",
			},
		},
	}
}

func backfillExecute(d *schema.ResourceData, meta interface{}) error {
	metaStruct := meta.(*ProviderMeta)
	client := metaStruct.TitanicClient
	connectorName := d.Get("connector_name").(string)
	backfillID := d.Get("backfill_id").(string)

	current, err := findBackfill(client, connectorName, backfillID)
	if err != nil {
		return err
	}

	if current == nil {
		return fmt.Errorf(
			"backfill %s does not exist for connector %s",
			backfillID,
			connectorName,
		)
	}

	// Adopt backfills that Titanic has already run into state untouched, so that
	// bringing existing backfills under Terraform does not re-snapshot them.
	if current.Status != statusPending {
		log.Printf(
			"[INFO] not executing backfill %s, Titanic reports status %q",
			backfillID,
			current.Status,
		)
		d.SetId(backfillID)

		return d.Set("status", current.Status)
	}

	if err := client.ExecuteBackfill(context.Background(), backfillID); err != nil {
		return fmt.Errorf("could not execute backfill %s: %w", backfillID, err)
	}

	d.SetId(backfillID)

	return backfillRead(d, meta)
}

func backfillRead(d *schema.ResourceData, meta interface{}) error {
	metaStruct := meta.(*ProviderMeta)
	client := metaStruct.TitanicClient
	connectorName := d.Get("connector_name").(string)
	backfillID := d.Id()

	// State written before connector_name was part of the schema cannot be looked
	// up in Titanic. Leave it alone instead of failing the whole run.
	if connectorName == "" {
		log.Printf(
			"[WARN] backfill %s has no connector_name in state, skipping refresh",
			backfillID,
		)

		return nil
	}

	current, err := findBackfill(client, connectorName, backfillID)
	if err != nil {
		return err
	}

	if current == nil {
		// The resource ID is deliberately kept so an already executed backfill is
		// never executed a second time.
		log.Printf(
			"[WARN] backfill %s is no longer listed for connector %s",
			backfillID,
			connectorName,
		)

		return d.Set("status", statusNotFound)
	}

	return d.Set("status", current.Status)
}

func findBackfill(
	client Titanic,
	connectorName string,
	backfillID string,
) (*titanic.Backfill, error) {
	backfills, err := client.GetBackfillsByConnectorName(context.Background(), connectorName)
	if err != nil {
		return nil, fmt.Errorf(
			"could not list backfills for connector %s: %w",
			connectorName,
			err,
		)
	}

	for _, backfill := range backfills {
		if backfill.ID == backfillID {
			return &backfill, nil
		}
	}

	return nil, nil
}

// backfillImport adopts an existing backfill by its UUID alone. The connector name
// is looked up in Titanic rather than being part of the import ID.
func backfillImport(
	d *schema.ResourceData,
	meta interface{},
) ([]*schema.ResourceData, error) {
	metaStruct := meta.(*ProviderMeta)
	client := metaStruct.TitanicClient
	backfillID := d.Id()

	backfill, err := client.GetBackfill(context.Background(), backfillID)
	if err != nil {
		return nil, fmt.Errorf("could not read backfill %s: %w", backfillID, err)
	}

	if err := d.Set("backfill_id", backfill.ID); err != nil {
		return nil, err
	}

	if err := d.Set("connector_name", backfill.ConnectorName); err != nil {
		return nil, err
	}

	if err := d.Set("status", backfill.Status); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}

func backfillDelete(d *schema.ResourceData, meta interface{}) error {
	// Operational trigger only. No remote infrastructure destruction required.
	return nil
}
