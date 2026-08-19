package titanic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

const (
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
		output := map[string]any{}
		if err := json.NewDecoder(body).Decode(&output); err != nil {
			return fmt.Errorf("unable to decode response: %w", err)
		}

		return fmt.Errorf("%w %v ", errHttpClient, output["message"])

	}
	return nil
}
