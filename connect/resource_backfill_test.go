package connect

import (
	"context"
	"errors"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBackfillExecutor struct {
	calls        int
	executedID   string
	executeError error
}

func (f *fakeBackfillExecutor) ExecuteBackfill(
	_ context.Context,
	backfillID string,
) error {
	f.calls++
	f.executedID = backfillID

	return f.executeError
}

func newBackfillResourceData(
	t *testing.T,
	backfillID string,
) *schema.ResourceData {
	t.Helper()

	resource := kafkaConnectBackfillResource()

	return schema.TestResourceDataRaw(
		t,
		resource.Schema,
		map[string]interface{}{
			"backfill_id": backfillID,
		},
	)
}

func TestKafkaBackfillResourceSchema(t *testing.T) {
	resource := kafkaConnectBackfillResource()

	require.NotNil(t, resource)
	require.NotNil(t, resource.Schema)

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

	assert.NotNil(t, resource.Create)
	assert.NotNil(t, resource.Read)
	assert.NotNil(t, resource.Delete)
}

func TestBackfillExecuteSuccess(t *testing.T) {
	const backfillID = "backfill-123"

	client := &fakeBackfillExecutor{}
	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, backfillID)

	err := backfillExecute(data, meta)

	require.NoError(t, err)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, backfillID, client.executedID)
	assert.Equal(t, backfillID, data.Id())
}

func TestBackfillExecuteError(t *testing.T) {
	const backfillID = "backfill-123"

	expectedErr := errors.New("could not execute backfill: Titanic returned an errorwith id: backfill-123")

	client := &fakeBackfillExecutor{
		executeError: expectedErr,
	}

	meta := &ProviderMeta{
		TitanicClient: client,
	}

	data := newBackfillResourceData(t, backfillID)

	err := backfillExecute(data, meta)

	require.Error(t, err)
	assert.ErrorIs(t, err, expectedErr)
	assert.EqualError(
		t,
		err,
		`could not execute backfill: Titanic returned an errorwith id: backfill-123`,
	)
	assert.Equal(t, 1, client.calls)
	assert.Equal(t, backfillID, client.executedID)

	// The resource ID must not be set when execution fails.
	assert.Empty(t, data.Id())
}

func TestBackfillReadClearsResourceID(t *testing.T) {
	const backfillID = "backfill-123"

	data := newBackfillResourceData(t, backfillID)
	data.SetId(backfillID)

	err := backfillRead(data, nil)

	require.NoError(t, err)
	assert.Empty(t, data.Id())
}

func TestBackfillDeleteDoesNothing(t *testing.T) {
	const backfillID = "backfill-123"

	data := newBackfillResourceData(t, backfillID)
	data.SetId(backfillID)

	err := backfillDelete(data, nil)

	require.NoError(t, err)

	// backfillDelete is intentionally a no-op.
	assert.Equal(t, backfillID, data.Id())
}
