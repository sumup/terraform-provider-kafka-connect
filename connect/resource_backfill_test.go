package connect

import (
	"context"
	"errors"
	"testing"

	"github.com/Mongey/terraform-provider-kafka-connect/titanic"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBackfillExecutor struct {
	calls        int
	executedID   string
	executeError error
	listedName   string
	listResult   []titanic.Backfill
	listError    error
	getID        string
	getResult    *titanic.Backfill
	getError     error
}

func (f *fakeBackfillExecutor) GetBackfill(
	_ context.Context,
	backfillID string,
) (*titanic.Backfill, error) {
	f.getID = backfillID

	return f.getResult, f.getError
}

func (f *fakeBackfillExecutor) ExecuteBackfill(
	_ context.Context,
	backfillID string,
) error {
	f.calls++
	f.executedID = backfillID

	return f.executeError
}

func (f *fakeBackfillExecutor) GetBackfillsByConnectorName(
	_ context.Context,
	connectorName string,
) ([]titanic.Backfill, error) {
	f.listedName = connectorName

	return f.listResult, f.listError
}

func newBackfillResourceData(
	t *testing.T,
	connectorName string,
	backfillID string,
) *schema.ResourceData {
	t.Helper()

	resource := kafkaConnectBackfillResource()

	return schema.TestResourceDataRaw(
		t,
		resource.Schema,
		map[string]interface{}{
			"connector_name": connectorName,
			"backfill_id":    backfillID,
		},
	)
}

func TestKafkaBackfillResourceSchema(t *testing.T) {
	resource := kafkaConnectBackfillResource()

	require.NotNil(t, resource)
	require.NotNil(t, resource.Schema)

	connectorName, ok := resource.Schema["connector_name"]
	require.True(t, ok, "connector_name schema field should exist")
	require.NotNil(t, connectorName)

	assert.Equal(t, schema.TypeString, connectorName.Type)
	assert.True(t, connectorName.Required)
	assert.True(t, connectorName.ForceNew)
	assert.Equal(
		t,
		"The Kafka Connect connector name whose backfills should be managed.",
		connectorName.Description,
	)

	backfillID, ok := resource.Schema["backfill_id"]
	require.True(t, ok, "backfill_id schema field should exist")
	require.NotNil(t, backfillID)

	assert.Equal(t, schema.TypeString, backfillID.Type)
	assert.True(t, backfillID.Required)
	assert.True(t, backfillID.ForceNew)
	assert.Equal(
		t,
		"The UUID of the backfill task to execute.",
		backfillID.Description,
	)

	status, ok := resource.Schema["status"]
	require.True(t, ok, "status schema field should exist")
	require.NotNil(t, status)

	assert.Equal(t, schema.TypeString, status.Type)
	assert.True(t, status.Computed)
	assert.False(t, status.Required)

	assert.NotNil(t, resource.Create)
	assert.NotNil(t, resource.Read)
	assert.NotNil(t, resource.Delete)
	require.NotNil(t, resource.Importer)
	assert.NotNil(t, resource.Importer.State)
}

func TestBackfillImport(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		getResult: &titanic.Backfill{
			ID:            backfillID,
			ConnectorName: connectorName,
			Status:        "completed",
		},
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, "", "")
	data.SetId(backfillID)

	imported, err := backfillImport(data, meta)

	require.NoError(t, err)
	require.Len(t, imported, 1)
	assert.Equal(t, backfillID, client.getID)
	assert.Equal(t, backfillID, imported[0].Id())
	assert.Equal(t, backfillID, imported[0].Get("backfill_id"))
	assert.Equal(t, connectorName, imported[0].Get("connector_name"))
	assert.Equal(t, "completed", imported[0].Get("status"))
}

func TestBackfillImportError(t *testing.T) {
	const backfillID = "backfill-123"

	client := &fakeBackfillExecutor{
		getError: errors.New("404 page not found"),
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, "", "")
	data.SetId(backfillID)

	imported, err := backfillImport(data, meta)

	require.Error(t, err)
	assert.Nil(t, imported)
	assert.EqualError(t, err, `could not read backfill backfill-123: 404 page not found`)
}

func TestBackfillExecuteSuccess(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listResult: []titanic.Backfill{
			{ID: backfillID, ConnectorName: connectorName, Status: statusPending},
		},
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)

	err := backfillExecute(data, meta)

	require.NoError(t, err)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, backfillID, client.executedID)
	assert.Equal(t, backfillID, data.Id())
}

// Backfills Titanic has already run must be adopted into state without being
// executed again.
func TestBackfillExecuteSkipsAlreadyRunBackfills(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	for _, status := range []string{"completed", "in_progress", "failed", "stopped", "paused"} {
		t.Run(status, func(t *testing.T) {
			client := &fakeBackfillExecutor{
				listResult: []titanic.Backfill{
					{ID: backfillID, ConnectorName: connectorName, Status: status},
				},
			}
			meta := &ProviderMeta{
				TitanicClient: client,
			}

			data := newBackfillResourceData(t, connectorName, backfillID)

			err := backfillExecute(data, meta)

			require.NoError(t, err)
			assert.Zero(t, client.calls, "backfill must not be executed")
			assert.Equal(t, backfillID, data.Id())
			assert.Equal(t, status, data.Get("status"))
		})
	}
}

func TestBackfillExecuteUnknownBackfill(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listResult: []titanic.Backfill{
			{ID: "other-backfill", ConnectorName: connectorName, Status: statusPending},
		},
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)

	err := backfillExecute(data, meta)

	require.Error(t, err)
	assert.EqualError(
		t,
		err,
		`backfill backfill-123 does not exist for connector snapshot-tester`,
	)
	assert.Zero(t, client.calls)
	assert.Empty(t, data.Id())
}

func TestBackfillExecuteError(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listResult: []titanic.Backfill{
			{ID: backfillID, ConnectorName: connectorName, Status: statusPending},
		},
		executeError: errors.New("Titanic returned an error"),
	}

	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)

	err := backfillExecute(data, meta)

	require.Error(t, err)
	assert.EqualError(
		t,
		err,
		`could not execute backfill backfill-123: Titanic returned an error`,
	)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, backfillID, client.executedID)

	// The resource ID must not be set when execution fails.
	assert.Empty(t, data.Id())
}

func TestBackfillExecuteListError(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listError: errors.New("Titanic returned an error"),
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)

	err := backfillExecute(data, meta)

	require.Error(t, err)
	assert.EqualError(
		t,
		err,
		`could not list backfills for connector snapshot-tester: Titanic returned an error`,
	)
	assert.Zero(t, client.calls)
	assert.Empty(t, data.Id())
}

func TestBackfillReadKeepsResourceWhenListed(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listResult: []titanic.Backfill{
			{ID: "other-backfill", ConnectorName: connectorName, Status: "succeeded"},
			{ID: backfillID, ConnectorName: connectorName, Status: "in_progress"},
		},
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)
	data.SetId(backfillID)

	err := backfillRead(data, meta)

	require.NoError(t, err)
	assert.Equal(t, connectorName, client.listedName)
	assert.Equal(t, backfillID, data.Id())
	assert.Equal(t, "in_progress", data.Get("status"))
}

// An executed backfill that Titanic no longer lists must stay in state, otherwise
// the next apply would execute it again.
func TestBackfillReadKeepsResourceWhenMissing(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listResult: []titanic.Backfill{
			{ID: "other-backfill", ConnectorName: connectorName, Status: "succeeded"},
		},
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)
	data.SetId(backfillID)

	err := backfillRead(data, meta)

	require.NoError(t, err)
	assert.Equal(t, connectorName, client.listedName)
	assert.Equal(t, backfillID, data.Id())
	assert.Equal(t, statusNotFound, data.Get("status"))
}

// State written before connector_name existed must survive a refresh.
func TestBackfillReadSkipsWhenConnectorNameMissing(t *testing.T) {
	const backfillID = "backfill-123"

	client := &fakeBackfillExecutor{}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, "", backfillID)
	data.SetId(backfillID)

	err := backfillRead(data, meta)

	require.NoError(t, err)
	assert.Empty(t, client.listedName, "Titanic must not be queried without a connector name")
	assert.Equal(t, backfillID, data.Id())
}

func TestBackfillReadError(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	client := &fakeBackfillExecutor{
		listError: errors.New("Titanic returned an error"),
	}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, connectorName, backfillID)
	data.SetId(backfillID)

	err := backfillRead(data, meta)

	require.Error(t, err)
	assert.EqualError(
		t,
		err,
		`could not list backfills for connector snapshot-tester: Titanic returned an error`,
	)
	assert.Equal(t, backfillID, data.Id())
}

func TestBackfillDeleteDoesNothing(t *testing.T) {
	const (
		connectorName = "snapshot-tester"
		backfillID    = "backfill-123"
	)

	data := newBackfillResourceData(t, connectorName, backfillID)
	data.SetId(backfillID)

	err := backfillDelete(data, nil)

	require.NoError(t, err)

	// backfillDelete is intentionally a no-op.
	assert.Equal(t, backfillID, data.Id())
}
