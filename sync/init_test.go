// go test github.com/homemade/fez/sync -v -run TestDefaultModifier
package sync

import (
	"testing"

	"github.com/tidwall/gjson"
)

func init() {
	gjson.AddModifier("default", func(json, arg string) string {
		if json == "" || json == "null" {
			return arg
		}
		return json
	})
}

func TestDefaultModifier(t *testing.T) {

	t.Run("missing field returns default", func(t *testing.T) {
		result := gjson.Get(`{"name": "test"}`, `amount|@default:0`)
		if result.Int() != 0 {
			t.Errorf("expected 0 but got %d", result.Int())
		}
	})

	t.Run("null field returns default", func(t *testing.T) {
		result := gjson.Get(`{"amount": null}`, `amount|@default:0`)
		if result.Int() != 0 {
			t.Errorf("expected 0 but got %d", result.Int())
		}
	})

	t.Run("present value returns value", func(t *testing.T) {
		result := gjson.Get(`{"amount": 5000}`, `amount|@default:0`)
		if result.Int() != 5000 {
			t.Errorf("expected 5000 but got %d", result.Int())
		}
	})

}
