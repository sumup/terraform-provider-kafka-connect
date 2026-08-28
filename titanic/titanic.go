package titanic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errHttpClient = errors.New("request failed with message")

type HTTPClient interface {
	Do(request *http.Request) (*http.Response, error)
}

type Client struct {
	httpClient HTTPClient
	baseURL    *url.URL
}

type Backfill struct {
	ID            string `json:"id"`
	ConnectorName string `json:"connector_name"`
	Status        string `json:"status"`
}

const (
	listPath    = "v1/backfills"
	getPath     = "v1/backfills/%s"
	executePath = "v1/backfills/%s/execute"
)

func NewClient(base string) (*Client, error) {
	parsedBase, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}

	return &Client{
		baseURL: parsedBase,
		httpClient: &http.Client{
			Timeout: time.Second * 30, //nolint:mnd
		},
	}, nil
}

func (c *Client) GetBackfill(ctx context.Context, id string) (*Backfill, error) {
	targetURL := c.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf(getPath, id),
	}).String()

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		targetURL,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating GET request context: %w", err)
	}

	httpReq.Header.Set("Accept", "application/json")

	res, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()

	errResponse := handleHttpStatus(res.StatusCode, res.Body)
	if errResponse != nil {
		return nil, errResponse
	}

	backfill := Backfill{}
	if err := json.NewDecoder(res.Body).Decode(&backfill); err != nil {
		return nil, fmt.Errorf("unable to decode response: %w", err)
	}

	return &backfill, nil
}

func (c *Client) GetBackfillsByConnectorName(
	ctx context.Context,
	connectorName string,
) ([]Backfill, error) {
	targetURL := c.baseURL.ResolveReference(&url.URL{
		Path: listPath,
		RawQuery: url.Values{
			"connector_name": {connectorName},
		}.Encode(),
	}).String()

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		targetURL,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed creating GET request context: %w", err)
	}

	httpReq.Header.Set("Accept", "application/json")

	res, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()

	errResponse := handleHttpStatus(res.StatusCode, res.Body)
	if errResponse != nil {
		return nil, errResponse
	}

	var backfills []Backfill
	if err := json.NewDecoder(res.Body).Decode(&backfills); err != nil {
		return nil, fmt.Errorf("unable to decode response: %w", err)
	}

	return backfills, nil
}

func (c *Client) ExecuteBackfill(ctx context.Context, id string) error {
	targetURL := c.baseURL.ResolveReference(&url.URL{
		Path: fmt.Sprintf(executePath, id),
	}).String()

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		targetURL,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed creating PUT request context: %w", err)
	}

	httpReq.Header.Set("Accept", "application/json")

	res, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer res.Body.Close()

	errResponse := handleHttpStatus(res.StatusCode, res.Body)
	if errResponse != nil {
		return errResponse
	}

	return nil
}

func handleHttpStatus(status int, body io.ReadCloser) error {
	switch status {
	case http.StatusMethodNotAllowed, http.StatusNotFound, http.StatusConflict,
		http.StatusTooManyRequests, http.StatusRequestTimeout, http.StatusGatewayTimeout,
		http.StatusInternalServerError, http.StatusBadRequest:
		raw, err := io.ReadAll(body)
		if err != nil {
			return fmt.Errorf("unable to read response: %w", err)
		}

		// Titanic reports some failures, such as validation errors, as plain text
		// rather than as a JSON body.
		output := map[string]any{}
		if err := json.Unmarshal(raw, &output); err != nil {
			return fmt.Errorf("%w %s", errHttpClient, strings.TrimSpace(string(raw)))
		}

		return fmt.Errorf("%w %v ", errHttpClient, output["message"])

	}
	return nil
}
