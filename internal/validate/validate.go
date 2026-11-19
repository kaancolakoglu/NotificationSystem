package validate

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	SMSMaxContentChars   = 1600
	EmailMaxContentBytes = 100_000
	PushMaxContentBytes  = 4096
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)

var rules = map[string]func(recipient, content string) error{
	"sms":   sms,
	"email": email,
	"push":  push,
}

func Notification(channel, recipient, content string) error {
	rule, ok := rules[channel]
	if !ok {
		return errors.New("channel must be one of sms, email, push")
	}
	if content == "" {
		return errors.New("content must not be empty")
	}
	if strings.ContainsRune(recipient, 0) || strings.ContainsRune(content, 0) {
		return errors.New("recipient and content must not contain a NUL character")
	}
	return rule(recipient, content)
}

func sms(recipient, content string) error {
	if !e164.MatchString(recipient) {
		return errors.New("recipient must be a phone number in E.164 format, for example +905551234567")
	}
	if utf8.RuneCountInString(content) > SMSMaxContentChars {
		return fmt.Errorf("content must be at most %d characters", SMSMaxContentChars)
	}
	return nil
}

func email(recipient, content string) error {
	address, err := mail.ParseAddress(recipient)
	if err != nil || address.Address != recipient {
		return errors.New("recipient must be a plain email address, for example user@example.com")
	}
	if len(content) > EmailMaxContentBytes {
		return fmt.Errorf("content must be at most %d bytes", EmailMaxContentBytes)
	}
	return nil
}

func push(recipient, content string) error {
	if recipient == "" {
		return errors.New("recipient must be a device token")
	}
	if len(content) > PushMaxContentBytes {
		return fmt.Errorf("content must be at most %d bytes", PushMaxContentBytes)
	}
	return nil
}
