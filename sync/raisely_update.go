package sync

import (
	"context"
	"log"
	"net/http"

	"github.com/carlmjohnson/requests"
)

type UpdateRaiselyDataParams struct {
	RaiselyAPIKey     string
	P2PId             string
	Context           context.Context
	RaiselyAPIBuilder *requests.Builder
}

func UpdateRaiselyData(params UpdateRaiselyDataParams, json string) (int, error) {
	raiselyError := RaiselyError{}
	var result int
	err := params.RaiselyAPIBuilder.
		Patch().
		Pathf("/v3/profiles/%s", params.P2PId).
		Param("partial", "true").
		Bearer(params.RaiselyAPIKey).
		BodyBytes([]byte(json)).
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
