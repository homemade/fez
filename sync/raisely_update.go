package sync

import (
	"context"
	"log"
	"net/http"

	"github.com/carlmjohnson/requests"
)

type UpdateRaiselyDataParams struct {
	RaiselyAPIKey     string
	Request           UpdateRaiselyDataRequest
	Context           context.Context
	RaiselyAPIBuilder *requests.Builder
}

type UpdateRaiselyDataRequest struct {
	P2PId string
	JSON  string
}

func UpdateRaiselyData(params UpdateRaiselyDataParams) (int, error) {
	raiselyError := RaiselyError{}
	var result int
	err := params.RaiselyAPIBuilder.
		Patch().
		Pathf("/v3/profiles/%s", params.Request.P2PId).
		Param("partial", "true").
		Bearer(params.RaiselyAPIKey).
		BodyBytes([]byte(params.Request.JSON)).
		ContentType("application/json").
		ErrorJSON(&raiselyError).
		Handle(func(response *http.Response) error {
			result = response.StatusCode
			return nil
		}).
		Fetch(params.Context)
	if err != nil {
		log.Printf("Raisely Error: %+v", raiselyError)
	}
	return result, err
}
