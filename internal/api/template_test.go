package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const badPlaceholderMessage = "content has double braces that are not a placeholder: write {{name}}, with letters, digits and underscores only"

func quoted(t *testing.T, text string) string {
	t.Helper()
	encoded, err := json.Marshal(text)
	if err != nil {
		t.Fatalf("encode %q: %v", text, err)
	}
	return string(encoded)
}

func createTemplate(t *testing.T, mux *http.ServeMux, content string) uuid.UUID {
	t.Helper()
	rec := postJSON(t, mux, "/v1/templates", `{"content":`+quoted(t, content)+`}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create template: status = %d, want %d, body %s", rec.Code, http.StatusCreated, rec.Body)
	}
	return decodeBody[api.Template](t, rec).Id
}

func fromTemplate(t *testing.T, id uuid.UUID, otherFields string) string {
	t.Helper()
	return `{"recipient":"+905551234567","channel":"sms","template_id":"` + id.String() + `"` + otherFields + `}`
}

func TestCreateTemplate(t *testing.T) {
	mux, pool := newTestServer(t)

	t.Run("valid content is stored", func(t *testing.T) {
		for _, content := range []string{"Hello {{firstName}}, your code is {{code_1}}.", "no placeholder", "one {brace} and {{{three}}}", "{{a}}{{b}}"} {
			rec := postJSON(t, mux, "/v1/templates", `{"content":`+quoted(t, content)+`}`, nil)

			if rec.Code != http.StatusCreated {
				t.Fatalf("content %q: status = %d, want %d, body %s", content, rec.Code, http.StatusCreated, rec.Body)
			}
			got := decodeBody[api.Template](t, rec)
			if got.Id.Version() != 7 || got.Content != content || got.CreatedAt.IsZero() {
				t.Errorf("content %q: template = %+v, want a version 7 id, the content and a creation time", content, got)
			}
			var stored string
			if err := pool.QueryRow(t.Context(), "SELECT content FROM templates WHERE id = $1", got.Id).Scan(&stored); err != nil || stored != content {
				t.Errorf("content %q: stored content = %q, error %v, want the content", content, stored, err)
			}
		}
	})

	t.Run("content that cannot be a template is 422", func(t *testing.T) {
		var before int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM templates").Scan(&before); err != nil {
			t.Fatalf("count templates: %v", err)
		}
		tests := []struct {
			body        string
			wantMessage string
		}{
			{body: `{"content":""}`, wantMessage: "content must not be empty"},
			{body: `{}`, wantMessage: "content must not be empty"},
			{body: `{"content":"a\u0000b"}`, wantMessage: "content must not contain a NUL character"},
			{body: `{"content":"Hello {{ name }}"}`, wantMessage: badPlaceholderMessage},
			{body: `{"content":"Hello {{first-name}}"}`, wantMessage: badPlaceholderMessage},
			{body: `{"content":"Hello {{name"}`, wantMessage: badPlaceholderMessage},
			{body: `{"content":"Hello name}}"}`, wantMessage: badPlaceholderMessage},
			{body: `{"content":"Hello {{}}"}`, wantMessage: badPlaceholderMessage},
			{body: `{"content":"Hello {{{{name}}}}"}`, wantMessage: badPlaceholderMessage},
		}
		for _, tt := range tests {
			wantError(t, postJSON(t, mux, "/v1/templates", tt.body, nil), http.StatusUnprocessableEntity, tt.wantMessage)
		}
		var after int
		if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM templates").Scan(&after); err != nil || after != before {
			t.Errorf("%d templates stored, error %v, want none", after-before, err)
		}
	})

	t.Run("a body that is not a template request is 400", func(t *testing.T) {
		wantError(t, postJSON(t, mux, "/v1/templates", `{"content":"hello","name":"welcome"}`, nil), http.StatusBadRequest, `field "name" is not known`)
		wantError(t, postJSON(t, mux, "/v1/templates", `{"content":5}`, nil), http.StatusBadRequest, `field "content" has the wrong type`)
		wantError(t, postJSON(t, mux, "/v1/templates", `not json`, nil), http.StatusBadRequest, "request body must be a valid JSON object")
		wantError(t, postJSON(t, mux, "/v1/templates?limit=1", `{"content":"hello"}`, nil), http.StatusBadRequest, `parameter "limit" is not known`)
	})
}

func TestCreateNotificationFromATemplate(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)
	greeting := createTemplate(t, mux, "Hi {{name}}, your code is {{code}}. Bye {{name}}.")
	onlyValue := createTemplate(t, mux, "{{value}}")
	plain := createTemplate(t, mux, "no placeholder")
	braces := createTemplate(t, mux, "{{{firstName}}} has {{code_1}} {points}")
	repeated := createTemplate(t, mux, strings.Repeat("{{value}}", 11))

	t.Run("the rendered content is stored and returned", func(t *testing.T) {
		tests := []struct {
			name        string
			template    uuid.UUID
			otherFields string
			wantContent string
		}{
			{name: "every placeholder", template: greeting, otherFields: `,"variables":{"name":"Ada","code":"4217"}`, wantContent: "Hi Ada, your code is 4217. Bye Ada."},
			{name: "a variable without a placeholder is not used", template: greeting, otherFields: `,"variables":{"name":"Ada","code":"4217","city":"Izmir"}`, wantContent: "Hi Ada, your code is 4217. Bye Ada."},
			{name: "a value is not rendered again", template: greeting, otherFields: `,"variables":{"name":"{{code}}","code":"{{name}}"}`, wantContent: "Hi {{code}}, your code is {{name}}. Bye {{code}}."},
			{name: "an empty value", template: greeting, otherFields: `,"variables":{"name":"","code":"4217"}`, wantContent: "Hi , your code is 4217. Bye ."},
			{name: "no placeholder and no variables", template: plain, otherFields: "", wantContent: "no placeholder"},
			{name: "no placeholder and empty variables", template: plain, otherFields: `,"variables":{}`, wantContent: "no placeholder"},
			{name: "empty content next to the template", template: plain, otherFields: `,"content":""`, wantContent: "no placeholder"},
			{name: "names with capitals, digits and underscores, next to single braces", template: braces, otherFields: `,"variables":{"firstName":"Ada","code_1":"7"}`, wantContent: "{Ada} has 7 {points}"},
			{name: "a null value without a placeholder is not used", template: plain, otherFields: `,"variables":{"city":null}`, wantContent: "no placeholder"},
			{name: "a placeholder used many times", template: repeated, otherFields: `,"variables":{"value":"ab"}`, wantContent: strings.Repeat("ab", 11)},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rec := postNotification(t, mux, fromTemplate(t, tt.template, tt.otherFields))

				if rec.Code != http.StatusAccepted {
					t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
				}
				got := decodeBody[api.Notification](t, rec)
				if got.Content != tt.wantContent {
					t.Errorf("content = %q, want %q", got.Content, tt.wantContent)
				}
				row, err := queries.GetNotification(t.Context(), got.Id)
				if err != nil || row.Content != tt.wantContent {
					t.Errorf("stored content = %q, error %v, want %q", row.Content, err, tt.wantContent)
				}
			})
		}
	})

	t.Run("a request that cannot be rendered or is not valid after it is 422", func(t *testing.T) {
		before := countNotifications(t, pool)
		long := `,"variables":{"value":"` + strings.Repeat("a", 1601) + `"}`
		tests := []struct {
			name        string
			body        string
			wantMessage string
		}{
			{name: "one variable missing", body: fromTemplate(t, greeting, `,"variables":{"name":"Ada"}`), wantMessage: "variables are missing: code"},
			{name: "every variable missing", body: fromTemplate(t, greeting, ""), wantMessage: "variables are missing: name, code"},
			{name: "null variables", body: fromTemplate(t, greeting, `,"variables":null`), wantMessage: "variables are missing: name, code"},
			{name: "a null value", body: fromTemplate(t, greeting, `,"variables":{"name":null,"code":"1"}`), wantMessage: "variables are missing: name"},
			{name: "variable name in another case", body: fromTemplate(t, greeting, `,"variables":{"Name":"Ada","code":"1"}`), wantMessage: "variables are missing: name"},
			{name: "unknown template", body: fromTemplate(t, uuid.Must(uuid.NewV7()), ""), wantMessage: "template not found"},
			{name: "content and template", body: fromTemplate(t, plain, `,"content":"hello"`), wantMessage: "send content or template_id, not both"},
			{name: "variables without a template", body: `{"recipient":"+905551234567","channel":"sms","content":"hello","variables":{"name":"Ada"}}`, wantMessage: "variables need a template_id"},
			{name: "neither content nor template", body: `{"recipient":"+905551234567","channel":"sms"}`, wantMessage: "content must not be empty"},
			{name: "rendered content is empty", body: fromTemplate(t, onlyValue, `,"variables":{"value":""}`), wantMessage: "content must not be empty"},
			{name: "rendered content is too long", body: fromTemplate(t, onlyValue, long), wantMessage: "content must be at most 1600 characters"},
			{name: "rendered content has a NUL character", body: fromTemplate(t, onlyValue, `,"variables":{"value":"a\u0000b"}`), wantMessage: "recipient and content must not contain a NUL character"},
			{name: "rendering is stopped at the largest content any channel allows", body: fromTemplate(t, repeated, `,"variables":{"value":"`+strings.Repeat("a", 10_000)+`"}`), wantMessage: "rendered content must be at most 100000 bytes"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				wantError(t, postNotification(t, mux, tt.body), http.StatusUnprocessableEntity, tt.wantMessage)
			})
		}
		if rows := countNotifications(t, pool) - before; rows != 0 {
			t.Errorf("%d notifications stored, want none", rows)
		}
	})

	t.Run("empty variables without a template are not variables", func(t *testing.T) {
		rec := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"hello","variables":{}}`)

		if rec.Code != http.StatusAccepted || decodeBody[api.Notification](t, rec).Content != "hello" {
			t.Errorf("answer = %d %s, want %d with the content hello", rec.Code, rec.Body, http.StatusAccepted)
		}
	})

	t.Run("a template_id or a value of the wrong type is 400", func(t *testing.T) {
		for _, id := range []string{"welcome", "", "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz", "urn:guid:019a0000-0000-7000-8000-000000000000"} {
			item := `{"recipient":"+905551234567","channel":"sms","template_id":"` + id + `"}`
			wantError(t, postNotification(t, mux, item), http.StatusBadRequest, "template_id must be a UUID")
			wantError(t, postBatch(t, mux, batchBody(item)), http.StatusBadRequest, "template_id must be a UUID")
		}
		wantError(t, postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","template_id":5}`), http.StatusBadRequest, `field "template_id" has the wrong type`)
		wantError(t, postNotification(t, mux, fromTemplate(t, greeting, `,"variables":{"name":"Ada","code":4217}`)), http.StatusBadRequest, `field "variables.code" has the wrong type`)
		wantError(t, postNotification(t, mux, fromTemplate(t, greeting, `,"variables":["Ada"]`)), http.StatusBadRequest, `field "variables" has the wrong type`)
	})
}

func TestTemplateThatCannotBeReadIsNotReportedAsMissing(t *testing.T) {
	mux, pool := newTestServer(t)
	template := createTemplate(t, mux, "hello")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/notifications", strings.NewReader(fromTemplate(t, template, "")))
	req.Header.Set("Content-Type", "application/json")

	mux.ServeHTTP(rec, req)

	wantError(t, rec, http.StatusInternalServerError, "internal error")
	if rows := countNotifications(t, pool); rows != 0 {
		t.Errorf("%d notifications stored, want none", rows)
	}
}

func TestCreateBatchFromTemplates(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)
	greeting := createTemplate(t, mux, "Hi {{name}}")
	farewell := createTemplate(t, mux, "Bye {{name}}")

	rec := postBatch(t, mux, batchBody(
		fromTemplate(t, greeting, `,"variables":{"name":"Ada"}`),
		fromTemplate(t, farewell, `,"variables":{"name":"Ada"}`),
		`{"recipient":"+905551234567","channel":"sms","content":"plain"}`,
		fromTemplate(t, greeting, `,"variables":{"name":"Grace"}`),
		fromTemplate(t, greeting, ""),
		fromTemplate(t, uuid.Must(uuid.NewV7()), ""),
		fromTemplate(t, greeting, `,"content":"hello","variables":{"name":"Ada"}`),
	))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	results := decodeBody[api.BatchCreated](t, rec).Results
	wantBatchResults(t, results, []string{"", "", "", "", "variables are missing: name", "template not found", "send content or template_id, not both"})
	for index, want := range []string{"Hi Ada", "Bye Ada", "plain", "Hi Grace"} {
		row, err := queries.GetNotification(t.Context(), *results[index].Id)
		if err != nil || row.Content != want {
			t.Errorf("item %d: stored content = %q, error %v, want %q", index, row.Content, err, want)
		}
	}
	if rows := countNotifications(t, pool); rows != 4 {
		t.Errorf("%d notifications stored, want 4", rows)
	}
}

func TestTemplateAndVariablesArePartOfAnIdempotentRequest(t *testing.T) {
	mux, pool := newTestServer(t)
	greeting := createTemplate(t, mux, "Hi {{name}}, {{title}}")
	farewell := createTemplate(t, mux, "Bye {{name}}, {{title}}")

	first := postNotification(t, mux, fromTemplate(t, greeting, `,"variables":{"name":"Ada","title":"Countess"}`), "templated")
	replay := postNotification(t, mux, fromTemplate(t, greeting, `,"variables":{"title":"Countess","name":"Ada"}`), "templated")
	otherValue := postNotification(t, mux, fromTemplate(t, greeting, `,"variables":{"name":"Grace","title":"Countess"}`), "templated")
	otherTemplate := postNotification(t, mux, fromTemplate(t, farewell, `,"variables":{"name":"Ada","title":"Countess"}`), "templated")
	sameText := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"Hi Ada, Countess"}`, "templated")

	if first.Code != http.StatusAccepted || replay.Code != http.StatusAccepted || first.Body.String() != replay.Body.String() {
		t.Errorf("replay with the variables in another order = %d %s, want the first answer, %d %s", replay.Code, replay.Body, first.Code, first.Body)
	}
	for name, rec := range map[string]*httptest.ResponseRecorder{"another value": otherValue, "another template": otherTemplate, "the rendered text as content": sameText} {
		if rec.Code != http.StatusConflict {
			t.Errorf("%s with the same key: status = %d, want %d", name, rec.Code, http.StatusConflict)
		}
	}
	if rows := countNotifications(t, pool); rows != 1 {
		t.Errorf("%d notifications stored, want 1", rows)
	}
}
