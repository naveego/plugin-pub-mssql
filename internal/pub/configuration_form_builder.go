package pub

import (
	"encoding/json"
	"fmt"
	jsonschema "github.com/naveego/go-json-schema"
)

type ConfigurationFormResponseBuilder struct {
	Response *ConfigurationFormResponse
	FormSchema *jsonschema.JSONSchema
	UISchema map[string]interface{}
	Data map[string]interface{}
}

func NewConfigurationFormResponseBuilder(req *ConfigurationFormRequest) *ConfigurationFormResponseBuilder {
	b := &ConfigurationFormResponseBuilder{
		Response:&ConfigurationFormResponse{
			DataJson: req.DataJson,
		},
	}

	if req.DataJson != "" {
		err := json.Unmarshal([]byte(req.DataJson), &b.Data)
		if err != nil {
			b.Response.Errors = []string{fmt.Sprintf("invalid data: %s",err.Error())}
		}
	}

	return b
}

func (c *ConfigurationFormResponseBuilder) Build() *ConfigurationFormResponse {

	if c.FormSchema != nil {
		j, _ := c.FormSchema.MarshalJSON()
		c.Response.SchemaJson = string(j)
	}
	if c.UISchema != nil {
		j, _ := json.Marshal(c.UISchema)
		c.Response.UiJson = string(j)
	}

	if c.Data != nil {
		j, _ := json.Marshal(c.Data)
		c.Response.DataJson = string(j)
	}

	return c.Response
}
