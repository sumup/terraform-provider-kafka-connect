package connect

import (
	"context"
	"crypto/tls"
	"log"

	"github.com/Mongey/terraform-provider-kafka-connect/titanic"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	kc "github.com/ricardo-ch/go-kafka-connect/lib/connectors"
)

type Titanic interface {
	ExecuteBackfill(ctx context.Context, id string) error
}

type ProviderMeta struct {
	KafkaClient   kc.HighLevelClient
	TitanicClient Titanic
}

func Provider() *schema.Provider {
	log.Printf("[INFO] Creating Provider")
	provider := schema.Provider{
		Schema: map[string]*schema.Schema{
			"url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_URL", ""),
			},
			"titanic_url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TITANIC_URL", ""),
				Description: "The base URL for the Titanic backfill service.",
			},
			"basic_auth_username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_USERNAME", ""),
			},
			"basic_auth_password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_PASSWORD", ""),
			},
			"tls_auth_crt": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_CRT", ""),
			},
			"tls_auth_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_KEY", ""),
			},
			"tls_auth_is_insecure": {
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_IS_INSECURE", ""),
			},
		},
		ConfigureFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"kafka-connect_connector": kafkaConnectorResource(),
			"kafka-connect_backfill":  kafkaConnectBackfillResource(),
		},
	}
	log.Printf("[INFO] Created provider: %v", provider)
	return &provider
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	log.Printf("[INFO] Initializing KafkaConnect client")
	addr := d.Get("url").(string)
	c := kc.NewClient(addr)
	user := d.Get("basic_auth_username").(string)
	pass := d.Get("basic_auth_password").(string)
	if user != "" && pass != "" {
		c.SetBasicAuth(user, pass)
	}

	crt := d.Get("tls_auth_crt").(string)
	key := d.Get("tls_auth_key").(string)
	is_insecure := d.Get("tls_auth_is_insecure").(bool)
	log.Printf("[INFO]Cert : %s\nKey: %s", crt, key)
	log.Printf("[INFO]SSl connection is insecure : %t", is_insecure)

	if is_insecure {
		c.SetInsecureSSL()
	} else {
		if crt != "" && key != "" {
			cert, err := tls.LoadX509KeyPair(crt, key)
			if err != nil {
				log.Fatalf("client: loadkeys: %s", err)
			}
			c.SetClientCertificates(cert)
		}
	}

	titanicURL := d.Get("titanic_url").(string)

	titanicClient, err := titanic.NewClient(titanicURL)
	if err != nil {
		log.Fatalf("can not create titanic client: %s", err)
	}

	meta := &ProviderMeta{
		KafkaClient:   c,
		TitanicClient: titanicClient,
	}

	return meta, nil
}
