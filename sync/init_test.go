// go test github.com/homemade/fez/sync -v -run TestDefaultModifier
package sync

import (
	"fmt"
	"net/url"
	"strings"
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

	gjson.AddModifier("pathJoinQueryURL", func(json, arg string) string {
		value := gjson.Parse(json)
		if !value.Exists() {
			return ""
		}
		parts := strings.SplitN(arg, ",", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return ""
		}
		u, err := url.Parse(parts[0])
		if err != nil {
			return ""
		}
		q := u.Query()
		q.Set(parts[1], value.String())
		u.RawQuery = q.Encode()
		return fmt.Sprintf(`"%s"`, u.String())
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

func TestPathJoinQueryURLModifier(t *testing.T) {

	t.Run("value becomes query param", func(t *testing.T) {
		got := gjson.Get(`{"token":"abc123"}`, `token|@pathJoinQueryURL:https://superswim.org.au/dashboard,access_token`).String()
		want := "https://superswim.org.au/dashboard?access_token=abc123"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("missing arg or paramName returns empty", func(t *testing.T) {
		for _, arg := range []string{"", "https://x.io/dashboard", "https://x.io/dashboard,", ",access_token"} {
			got := gjson.Get(`{"token":"abc"}`, `token|@pathJoinQueryURL:`+arg).String()
			if got != "" {
				t.Errorf("arg=%q: expected empty, got %q", arg, got)
			}
		}
	})

	t.Run("missing source value returns empty", func(t *testing.T) {
		got := gjson.Get(`{}`, `token|@pathJoinQueryURL:https://x.io/dashboard,access_token`).String()
		if got != "" {
			t.Errorf("expected empty, got %q", got)
		}
	})

	t.Run("value is url-escaped in query", func(t *testing.T) {
		got := gjson.Get(`{"token":"a b&c"}`, `token|@pathJoinQueryURL:https://x.io/dashboard,access_token`).String()
		want := "https://x.io/dashboard?access_token=a+b%26c"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})
}
