package validate_test

import (
	"strings"
	"testing"

	"github.com/kaancolakoglu/NotificationSystem/internal/validate"
)

func TestNotification(t *testing.T) {
	tests := []struct {
		name      string
		channel   string
		recipient string
		content   string
		wantErr   string
	}{
		{"sms ok", "sms", "+905551234567", "hello", ""},
		{"sms content at limit", "sms", "+905551234567", strings.Repeat("ş", validate.SMSMaxContentChars), ""},
		{"sms content over limit", "sms", "+905551234567", strings.Repeat("ş", validate.SMSMaxContentChars+1), "at most 1600 characters"},
		{"sms empty content", "sms", "+905551234567", "", "content must not be empty"},
		{"sms without plus", "sms", "905551234567", "hello", "E.164"},
		{"sms leading zero", "sms", "+05551234567", "hello", "E.164"},
		{"sms letters", "sms", "+90555abc4567", "hello", "E.164"},
		{"sms longest number", "sms", "+123456789012345", "hello", ""},
		{"sms shortest number", "sms", "+12", "hello", ""},
		{"sms too long", "sms", "+1234567890123456", "hello", "E.164"},
		{"sms too short", "sms", "+1", "hello", "E.164"},
		{"sms empty recipient", "sms", "", "hello", "E.164"},

		{"email ok", "email", "user@example.com", "hello", ""},
		{"email content at limit", "email", "user@example.com", strings.Repeat("a", validate.EmailMaxContentBytes), ""},
		{"email content over limit", "email", "user@example.com", strings.Repeat("a", validate.EmailMaxContentBytes+1), "at most 100000 bytes"},
		{"email multibyte over limit", "email", "user@example.com", strings.Repeat("ş", validate.EmailMaxContentBytes/2+1), "at most 100000 bytes"},
		{"email empty content", "email", "user@example.com", "", "content must not be empty"},
		{"email without at", "email", "user.example.com", "hello", "email address"},
		{"email without domain", "email", "user@", "hello", "email address"},
		{"email empty recipient", "email", "", "hello", "email address"},
		{"email with display name", "email", "Bob <user@example.com>", "hello", "email address"},
		{"email in angle brackets", "email", "<user@example.com>", "hello", "email address"},
		{"email with spaces around", "email", " user@example.com ", "hello", "email address"},
		{"email with comment", "email", "user@example.com (work)", "hello", "email address"},

		{"push ok", "push", "device-token-1", "hello", ""},
		{"push content at limit", "push", "device-token-1", strings.Repeat("a", validate.PushMaxContentBytes), ""},
		{"push content over limit", "push", "device-token-1", strings.Repeat("a", validate.PushMaxContentBytes+1), "at most 4096 bytes"},
		{"push multibyte over limit", "push", "device-token-1", strings.Repeat("ş", validate.PushMaxContentBytes/2+1), "at most 4096 bytes"},
		{"push empty content", "push", "device-token-1", "", "content must not be empty"},
		{"push empty recipient", "push", "", "hello", "device token"},

		{"nul in content", "sms", "+905551234567", "a\x00b", "NUL"},
		{"nul in recipient", "push", "token\x00", "hello", "NUL"},

		{"unknown channel", "fax", "+905551234567", "hello", "channel must be one of"},
		{"empty channel", "", "+905551234567", "hello", "channel must be one of"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate.Notification(tt.channel, tt.recipient, tt.content)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("no error, want one containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
