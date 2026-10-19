package httpapi

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

// openAPISpec documents every route; TestOpenAPISpecCoversEveryRoute keeps the two in step.
//
//go:embed openapi.yaml
var openAPISpec []byte

func serveOpenAPI(c *gin.Context) {
	c.Data(http.StatusOK, "application/yaml", openAPISpec)
}
